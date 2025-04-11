from typing import Dict, List, Any
import pyodata
import datetime
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting operations like checkpoint and upsert


class ODataClient:
    """Client for interacting with OData services using pyodata library."""
    def __init__(self, service_url: str, session, state: Dict = None):
        self.service_url = service_url
        self.state = state or {}
        try:
            self.client = pyodata.Client(service_url, session)
            log.info("OData Version 2 client initialized")
        except Exception as e:
            log.severe(f"Failed to initialize OData client: {str(e)}")
            raise ConnectionError(f"OData client initialization failed: {str(e)}")
        self.batch_requests = []


    def _extract_special_attributes(self, value):
        """Extract special attributes from an entity-like object."""
        extracted = {}
        for attr in dir(value):
            if not attr.startswith('_') and not callable(getattr(value, attr)):
                try:
                    extracted[attr] = self._extract_entity_data(value=getattr(value, attr))
                except Exception:
                    raise RuntimeError(f"Error extracting attribute {attr} from entity")
        return extracted


    def _exec_query(self, entity_set_obj, query_options: Dict = None) -> Any:
        """Execute a query on an entity set with optional query options."""
        query = entity_set_obj.get_entities()
        query = self._apply_query_options(query=query, query_options=query_options)
        entities = query.execute()
        return entities


    def _extract_entity_data(self, value):
        """Recursively convert EntityProxy objects and nested structures to dictionaries."""
        # Handle EntityProxy objects
        if hasattr(value, '_cache') and value._cache:
            return self._extract_entity_data(value=value._cache)

        # Handle lists
        elif isinstance(value, list):
            return [self._extract_entity_data(value=item) for item in value]

        # Handle dictionaries
        elif isinstance(value, dict):
            return {k: self._extract_entity_data(value=v) for k, v in value.items()}

        # Check for other entity-like objects with special attributes
        elif hasattr(value, '__dict__') and not isinstance(value, (str, int, float, bool)) and value is not None:
            extracted = self._extract_special_attributes(value=value)
            if extracted:
                return extracted

        return value


    def _upsert_formatted_data(self, entities, entity_set_obj, query_options,table, update_state):
        """Upsert formatted data from an entity set."""
        while True:
            for entity in entities:
                record = self._extract_entity_data(value=entity)

                if 'expand' in query_options:
                    self._process_expanded_entities(entity=entity, record=record, expand_options=query_options['expand'])

                record = self.clean_odata_fields(data=record)
                yield op.upsert(table=table, data=record)
                self._update_state_tracker(item=record, update_state=update_state)

            log.info(f"upserted {len(entities)} records to table {table}")

            yield op.checkpoint(self.state)

            if entities.next_url is None:
                break

            entities = entity_set_obj.get_entities().next_url(entities.next_url).execute()


    def upsert_entity(self, entity, state: Dict = None):
        """Fetch data from an entity set and yield records."""
        entity_set = entity.get('entity_set', None)
        query_options = entity.get('query_options', {})
        table = entity.get('table', entity_set)
        update_state = entity.get('update_state', None)

        if not entity_set:
            raise ValueError("entity_set must be specified")

        if state:
            self.state = state

        try:
            entity_set_obj = getattr(self.client.entity_sets, entity_set)
            entities = self._exec_query(entity_set_obj=entity_set_obj, query_options=query_options)
            log.info(f"Fetched data from entity set: {entity_set}")

            yield from self._upsert_formatted_data(entities=entities, entity_set_obj=entity_set_obj, query_options=query_options, table=table, update_state=update_state)
            return self.state

        except Exception as e:
            log.severe(f"Error fetching entity set {entity_set}: {str(e)}")
            raise ConnectionError(f"Error fetching entity set: {str(e)}")


    def upsert_multiple_entity(self, entity_list: List[Dict], state: Dict = None):
        """Fetch data from multiple entity sets and yield records."""
        if state:
            self.state = state
        for entity in entity_list:
            yield from self.upsert_entity(entity=entity)
        return self.state


    def process_expand_options(self, expand_dict, current_path='', expand_paths=None, select_paths=None):
        """Process expand options recursively, building proper paths for OData V2."""
        expand_paths = expand_paths or []
        select_paths = select_paths or []

        for nav_prop, options in expand_dict.items():
            # Build the full expansion path for this navigation property
            path = f"{current_path}{nav_prop}" if current_path else nav_prop
            expand_paths.append(path)

            # Add select fields for this expanded entity if specified
            if isinstance(options, dict) and 'select' in options:
                for field in options['select']:
                    select_paths.append(f"{path}/{field}")

            # Process nested expansions recursively
            if isinstance(options, dict) and 'expand' in options:
                expand_paths, select_paths = self.process_expand_options(
                    expand_dict=options['expand'],
                    current_path=f"{path}/",
                    expand_paths=expand_paths,
                    select_paths=select_paths
                )

        return expand_paths, select_paths


    def _apply_all_select_path(self, select_paths, query, query_options):
        """Apply select paths to a query, combining with any existing select options."""
        all_select_paths = []
        if 'select' in query_options:
            all_select_paths.extend(query_options['select'])
        if select_paths:
            all_select_paths.extend(select_paths)

        # Apply select if we have paths
        if all_select_paths:
            query = query.select(','.join(all_select_paths))
        return query


    def _apply_filter(self, query, query_options):
        """Apply a filter to a query if specified."""
        if 'filter' in query_options:
            filter_value = query_options['filter']
            if filter_value and isinstance(filter_value, str):
                query = query.filter(filter_value)
        return query


    def _apply_query_options(self, query, query_options: Dict):
        """Apply OData query options to a query with better validation and error handling."""
        try:
            if 'expand' in query_options:
                expand_paths, select_paths = self.process_expand_options(expand_dict=query_options['expand'])
                if expand_paths:
                    try:
                        query = query.expand(','.join(expand_paths))
                    except Exception as e:
                        log.warning(f"Error applying expand paths: {str(e)}")

                query = self._apply_all_select_path(select_paths=select_paths, query=query, query_options=query_options)

            elif 'select' in query_options:
                query = query.select(','.join(query_options['select']))

            query = self._apply_filter(query=query, query_options=query_options)

            if 'orderby' in query_options and query_options['orderby']:
                query = query.order_by(query_options['orderby'])

            if 'top' in query_options and query_options['top']:
                query = query.top(int(query_options['top']))

            if query_options.get('count', False):
                query = query.count(inline=True)

            return query
        except Exception as e:
            raise RuntimeError(f"Error applying query options: {str(e)}")


    def _update_state_tracker(self, item: Dict, update_state: Dict) -> Any:
        """Updates the state tracker with the highest values from the current record."""

        if not update_state:
            return

        for state_var, column in update_state.items():
            try:
                if column in item:
                    current_value = item[column]
                    if state_var not in self.state or current_value > self.state[state_var]:
                        self.state[state_var] = current_value
            except KeyError:
                log.severe(f"{column} not found in the fetched data while updating state")


    def _process_expanded_entities(self, entity, record: Dict, expand_options: Dict):
        if not entity or not isinstance(record, dict) or not expand_options:
            log.warning("Invalid parameters for processing expanded entities")
            return

        for nav_prop, options in expand_options.items():
            self._process_navigation_property(entity=entity, record=record, nav_prop=nav_prop, options=options)


    def _process_navigation_property(self, entity, record: Dict, nav_prop: str, options: Dict):
        """Process a single navigation property."""
        if not hasattr(entity, nav_prop):
            log.warning(f"Navigation property '{nav_prop}' not found in entity")
            return

        try:
            expanded_data = getattr(entity, nav_prop)

            # Handle null expanded data
            if expanded_data is None:
                record[nav_prop] = None
                return

            # Determine if it's a collection or single entity
            is_collection = hasattr(expanded_data, '__iter__') and not isinstance(expanded_data, dict)

            if is_collection:
                record[nav_prop] = self._process_expanded_collection(collection=expanded_data, options=options)
            else:
                record[nav_prop] = self._process_expanded_entity(entity=expanded_data, options=options)

        except Exception as e:
            log.severe(f"Error processing expanded entity {nav_prop}: {str(e)}")
            record[nav_prop] = None


    def _process_expanded_collection(self, collection, options: Dict) -> List:
        """Process a collection of expanded entities."""
        result = []

        for item in collection:
            try:
                # Extract item data using existing method
                item_dict = self._extract_entity_data(value=item)

                # Process nested expansions if defined
                if isinstance(options, dict) and 'expand' in options:
                    self._process_expanded_entities(entity=item, record=item_dict, expand_options=options['expand'])

                # Filter by select if specified
                if isinstance(options, dict) and 'select' in options:
                    item_dict = {k: v for k, v in item_dict.items() if k in options['select']}

                result.append(item_dict)

            except Exception as e:
                log.warning(f"Error processing collection item: {str(e)}")

        return result


    def _process_expanded_entity(self, entity, options: Dict) -> Dict:
        """Process a single expanded entity."""
        # Extract data using existing method
        entity_dict = self._extract_entity_data(value=entity)

        # Process nested expansions if defined
        if isinstance(options, dict) and 'expand' in options:
            self._process_expanded_entities(entity=entity, record=entity_dict, expand_options=options['expand'])

        # Filter by select if specified
        if isinstance(options, dict) and 'select' in options and entity_dict:
            entity_dict = {k: v for k, v in entity_dict.items() if k in options['select']}

        return entity_dict


    @staticmethod
    def clean_odata_fields(data):
        if isinstance(data, dict):
            return {k: ODataClient.clean_odata_fields(data=v) for k, v in data.items()
                    if not (isinstance(k, str) and '@odata' in k)}
        elif isinstance(data, list):
            return [ODataClient.clean_odata_fields(data=item) for item in data]
        elif isinstance(data, datetime.datetime):
            return data.isoformat()
        else:
            return data


    def add_batch(self, entity: Dict):
        """Add a batch request to the batch queue."""
        entity_set = entity.get('entity_set', None)

        if not entity_set:
            raise ValueError("entity_set must be specified")

        batch_request = {
            'entity_set': entity_set,
            'query_options': entity.get('query_options', {}),
            'table':  entity.get('table', entity_set),
            'update_state': entity.get('update_state', None)
        }

        self.batch_requests.append(batch_request)
        return self  # Enable method chaining


    def _build_batch(self, batch):
        """Adds the batche request to the client batch"""
        for batch_request in self.batch_requests:
            entity_set = batch_request['entity_set']
            query_options = batch_request['query_options']
            entity_set_obj = getattr(self.client.entity_sets, entity_set)
            query = entity_set_obj.get_entities()
            query = self._apply_query_options(query=query, query_options=query_options)
            batch.add_request(query)
            log.info("Added request to batch ")

        return batch


    def upsert_batch(self, state: Dict = None):
        """Execute a batch request with multiple parts."""
        if not self.batch_requests:
            log.warning("No batch requests to execute")
            return self.state

        if state:
            self.state = state

        log.info("created batch ")
        batch = self.client.create_batch()
        batch = self._build_batch(batch=batch)
        response = batch.execute()

        for index, entities in enumerate(response):
            entity_set_obj = getattr(self.client.entity_sets, self.batch_requests[index]['entity_set'])
            query_options = self.batch_requests[index]['query_options']
            table = self.batch_requests[index]['table']
            update_state = self.batch_requests[index]['update_state']

            yield from self._upsert_formatted_data(entities=entities, entity_set_obj=entity_set_obj, query_options=query_options, table=table, update_state=update_state)

        self.batch_requests = []
        return self.state
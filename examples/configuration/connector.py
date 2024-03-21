from fivetran_customer_sdk import Connector
from fivetran_customer_sdk import Operations as op
from cryptography.fernet import Fernet


encrypted_message = b'gAAAAABl-3QGKUHpdUhBNpnW1_SnSkQGrAwev-uBBJaZo4NmtylIMg8UX6usuG4Z-h80OvfJajW6HU56O5hofapEIh4W33vuMpJgq0q3qMQx6R3Ol4qZ3Wc2DyIIapxbK5BrQHshBF95'


def update(configuration: dict, state: dict):
    key = configuration['my_key']
    f = Fernet(key.encode())

    message = f.decrypt(encrypted_message)

    yield op.upsert(table="crypto", data={'msg': message})


connector = Connector(update=update)


if __name__ == "__main__":
    connector.debug(configuration={
        'my_key': 'EKlFpH8sZmdhhZ9lGhezgMTwAw3_Y2e7wbco7Gxt3SA='
    })

import aiochclient


async def client():
    return aiochclient.Client(
        url='http://localhost:8123',
        user='user',
        password='user',
        database='ai'
    )

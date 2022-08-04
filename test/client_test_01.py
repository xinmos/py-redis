from client import Client

if __name__ == '__main__':
    client = Client()
    client.mset('k1', 'v1', 'k2', ['v2-0', 1, 'v2-2'], 'k3', 'v3')
    print(client.get('k2'))
    print(client.mget('k3', 'k1'))
    client.delete('k1')
    print(client.get('k1'))
    client.delete('k1')
    client.set('kx', {'vx': {'vy': 0, 'vz': [1, 2, 3]}})
    print(client.get('kx'))
    client.flush()

from client import AppendDbClient
import time, string, random
db_handler = AppendDbClient()


# ## Write tests
cnt = 0
while True:
    cnt += 1
    for i in range(1, 10):
        s = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        r = db_handler.write_data(f'name_{i}', f'{cnt}__{s}_{i*i}')
        print(f'>> {i}_write_response: {r}')


# ## Read tests
# for i in range(1, 100):    
#     r = db_handler.read_data(f'name_{i}')
#     print(f'>>read_response: {r}')

# db_handler.delete_data('name')

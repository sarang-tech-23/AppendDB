from client import AppendDbClient

db_handler = AppendDbClient()

# db_handler.write_data('name', 'swaraj')

r = db_handler.read_data('name')
print(f'>>read_response: {r}')

# db_handler.update_data('name', 'sarang')
# db_handler.delete_data('name')

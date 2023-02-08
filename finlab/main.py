from finlab import data, login


print(login('HnyHspapktWgWVzmrieyK1kC+N2Gc3IDxbG+Xz5fYFt7XdbeMvodY5oQbkt5LA30#free')) # api token from https://ai.finlab.tw/api_token
close = data.get('price:收盤價', save_to_storage=True)
print(close.iloc[:5,:5])
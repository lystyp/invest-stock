import finlab

finlab.login('HnyHspapktWgWVzmrieyK1kC+N2Gc3IDxbG+Xz5fYFt7XdbeMvodY5oQbkt5LA30#free')
d = finlab.data.get('internal_equity_changes:發行股數')

print(d.iloc[-5:-1])
with open('admin.py', 'rb') as f:
    c = f.read()

# Pattern 1: is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL
old1 = b'is_supreme = current_user.get("email") == SUPREME_ADMIN_EMAIL'
new1 = b'is_supreme = current_user.get("is_supreme_admin")'

c1 = c.count(old1)
print(f'Pattern 1: {c1} occurrences')
if c1 > 0:
    c = c.replace(old1, new1)

# Pattern 2: OR combination
old2 = b'is_supreme = (current_user.get("email") == SUPREME_ADMIN_EMAIL or current_user.get("is_supreme_admin", False))'
new2 = b'is_supreme = current_user.get("is_supreme_admin", False)'

c2 = c.count(old2)
print(f'Pattern 2: {c2} occurrences')
if c2 > 0:
    c = c.replace(old2, new2)

# Pattern 3: route guards (4 instances)
old3 = b'if current_user.get("email") != SUPREME_ADMIN_EMAIL:'
new3 = b'if not current_user.get("is_supreme_admin"):'

c3 = c.count(old3)
print(f'Pattern 3: {c3} occurrences (will replace up to 4)')
replaced3 = 0
temp = c
for _ in range(4):
    idx = temp.find(old3)
    if idx >= 0:
        temp = temp[:idx] + new3 + temp[idx+len(old3):]
        replaced3 += 1
c = temp

c4 = c.count(b'SUPREME_ADMIN_EMAIL')
print(f'SUPREME_ADMIN_EMAIL remaining: {c4}')
print(f'Route guards replaced: {replaced3}/4')

with open('admin.py', 'wb') as f:
    f.write(c)
print('admin.py updated successfully')
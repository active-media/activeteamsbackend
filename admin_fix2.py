with open('admin.py', 'r') as f:
    c = f.read()

# Replace the OR-combination pattern
old2 = 'is_supreme = (current_user.get("email") == SUPREME_ADMIN_EMAIL or current_user.get("is_supreme_admin", False))'
new2 = 'is_supreme = current_user.get("is_supreme_admin", False)'

c2 = c.count(old2)
print(f'OR-combo pattern occurrences: {c2}')
if c2 > 0:
    c = c.replace(old2, new2)
    print('Replaced OR-combo patterns')

# Also check for the standalone pattern that might have a slightly different form
old3 = 'is_supreme = (current_user.get("email") == SUPREME_ADMIN_EMAIL or current_user.get("is_supreme_admin", False))'
# Check if there's a version without the parentheses or with different spacing

count_after = c.count('SUPREME_ADMIN_EMAIL')
print(f'SUPREME_ADMIN_EMAIL count after: {count_after}')

with open('admin.py', 'w') as f:
    f.write(c)
print('Done')
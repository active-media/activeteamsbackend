with open('admin.py', 'r') as f:
    c = f.read()

# Replace the 4-line OR-combo block (lines 918-921)
old_block1 = '''is_supreme = (
            current_user.get("email") == SUPREME_ADMIN_EMAIL
            or current_user.get("is_supreme_admin", False)
        )'''

new_block1 = '''is_supreme = current_user.get("is_supreme_admin")'''

count1 = c.count(old_block1)
print(f'Block 1 (get_all_users): {count1} occurrences')
if count1 > 0:
    c = c.replace(old_block1, new_block1)

# Replace the 4-line OR-combo block (lines 1108-1111)
old_block2 = '''is_supreme = (
            current_user.get("email") == SUPREME_ADMIN_EMAIL
            or current_user.get("is_supreme_admin", False)
        )'''

new_block2 = '''is_supreme = current_user.get("is_supreme_admin")'''

count2 = c.count(old_block2)
print(f'Block 2 (get_distinct_roles): {count2} occurrences')
if count2 > 0:
    c = c.replace(old_block2, new_block2)

count_after = c.count('SUPREME_ADMIN_EMAIL')
print(f'SUPREME_ADMIN_EMAIL count after: {count_after}')

with open('admin.py', 'w') as f:
    f.write(c)
print('Done')
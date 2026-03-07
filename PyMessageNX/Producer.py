from MessageNX import MessageNX


jobs = [
    {"name": "Rohit", "branch": "Computer Science"},
    {"name": "Ananya", "branch": "Electronics"},
    {"name": "Vikram", "branch": "Mechanical"},
    {"name": "Sneha", "branch": "Civil"},
]

mnx = MessageNX()

# Resister for id
# id = mnx.app_name("sample-app").register()
# print(id)

for job in jobs:
    res = mnx.produce(job, "141983614")
    print(res.content)

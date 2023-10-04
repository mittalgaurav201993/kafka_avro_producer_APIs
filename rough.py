import faker

fake = faker.Faker()

print(fake.date_time().time().second)

a = {'b', 1, 3}

for i in a:
    print(i)

a.add(1)
print(a)
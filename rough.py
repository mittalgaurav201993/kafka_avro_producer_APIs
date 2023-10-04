import faker

fake = faker.Faker()

print(fake.date_time().time().second)

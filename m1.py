import time
import json
import os
import sys
import bson


from datetime import datetime

from django.contrib.auth.hashers import make_password
from pymongo import MongoClient
from rc.bigtable import MetadataTable
from rc_web.database.models import WebappCustomer, WebappPlant


client = MongoClient(os.environ.get('MONGODB_URL'))
METADATA_PREFIX = 'field_data_01.'


def sync_metadata():
    all_metadata = MetadataTable.connect().get_all()
    sorted_metadata = {}

    for k, device_metadata in all_metadata.iteritems():
        plant_path = '.'.join(k.split('.')[1:3])

        if plant_path in sorted_metadata:
            sorted_metadata[plant_path].update({k: device_metadata})
        else:
            sorted_metadata.update({plant_path: {k: device_metadata}})

    db = client.get_database()

    #print db.collection_names(include_system_collections=False)
    db.drop_collection('metadata_temp')
    #print db.collection_names(include_system_collections=False)
    db.create_collection('metadata_temp')
    #print db.collection_names(include_system_collections=False)
    #for k, v in sorted_metadata.iteritems():
       # print 'working on {}'.format(k)
        #db.metadata_temp.insert_one({'path': k, 'data': json.dumps(v)})
        #print 'done with {}'.format(k)
    print ('Working on update metadata')
    db.metadata_temp.insert_many([{'path': k, 'data': json.dumps(v)} for k, v in sorted_metadata.iteritems()])
    db.metadata_temp.create_index('path')
    db.metadata_temp.rename('metadata', dropTarget=True)
    #db.drop_collection('metadata_temp')
    print ('Done  update metadata')
    print db.collection_names(include_system_collections=False)


def sync_users_plants(customers_logins):

    print('Running metadata export')
    sync_metadata()

    print('Running plants synchronization')

    # WebappCustomers  from PostgresDB ('rc_webapp_db_sync_webappcustomer' table)
    webapp_customers_all = WebappCustomer.objects.all()

    # customers_logins not empty
    webapp_customers = [
        webcustomer for webcustomer in webapp_customers_all if webcustomer.login in customers_logins
        ] if customers_logins else webapp_customers_all

    #client = MongoClient(os.environ.get('MONGODB_URL'))
    db = client.get_database()

    # users_collection  from  MongoDB ('auth_user' collection)
    users_collection = db['auth_user']
    max_id = 1 # id of last auth_user
    mongo_users_dict = {}  #users collection
    for user in users_collection.find():
        mongo_users_dict[user['username']] = user
        max_id = max(max_id, user['id'])

    # plants_collection  from  MongoDB ('plants ' collection)
    plants_collection = db['plants']
    mongo_plants_dict = {}
    for plant in plants_collection.find():
        mongo_plants_dict[plant['path']] = plant

    # collections  from  MongoDB ('data_sets', 'metadata', 'profiles' collection)
    data_sets = db['data_sets']
    metadata = db['metadata']
    profiles_collection = db['profiles']

    all_pants_paths =[]
    errors_dates = {}
    errors_metadata = []

    dataset_exists = lambda plant_path, day: ({
        'plant': '%s%s' % (METADATA_PREFIX , plant_path),
        'date': datetime(day.year, day.month, day.day),
        })

    mongo_users_update = lambda login, passwrd: (
        {'username': login},
        {'$set': {'password': make_password(passwrd)}}
        )

    mongo_users_insert = lambda u_id, login, password : bson.son.SON([
        ('id', u_id),
        ('password', make_password(password)),
        ('last_login', None),
        ('is_superuser', False),
        ('username', login),
        ('first_name', ''),
        ('last_name', ''),
        ('email', ''),
        ('is_staff', False),
        ('is_active', True),
        ('date_joined', datetime.now()),
        ])

    mongo_plant_update_is_date = lambda ppath, pname, pdate: (
        {'path': ppath},
        {'$set': {'plant_name': pname, 'date_to_show': pdate}}
        )

    mongo_plant_update_no_date = lambda ppath, pname: (
        {'path': ppath},
        {'$set': {'plant_name': pname}}
        )

    mongo_plant_insert_is_date = lambda ppath, pname, pdate: bson.son.SON([
        ('plant_name', pname),
        ('path', ppath),
        ('tariff', 0.0),
        ('date_to_show', pdate),
        ])

    mongo_plant_insert_no_date = lambda ppath, pname: bson.son.SON([
        ('plant_name', pname),
        ('path', ppath),
        ('tariff', 0.0),
        ])

    profiles_collection_update = lambda login, ppaths, is_admin: (
        {'user_name': login},
        {'$set': {'plants': ppaths, 'is_admin': is_admin}}
        )

    profiles_collection_insert = lambda login, ppaths, is_admin: bson.son.SON([
         ('user_name', login),
         ( 'plants', ppaths),
        ('is_admin', is_admin),
    ])

    for customer in webapp_customers:
        if customer.login in mongo_users_dict.keys():
            users_collection.update_one(*mongo_users_update(customer.login, customer.password))
        else:
            max_id += 1
            users_collection.insert_one(mongo_users_insert(max_id, customer.login, customer.password))

        plant_paths = []

        customer_plants = WebappPlant.objects.all().filter(webappcustomer=customer)

        for plant in customer_plants:
            if plant.date_to_show and data_sets.count(dataset_exists(plant.path, plant.date_to_show)) <= 0:
                print('skip {} because no dataset for {}'.format(plant.path, plant.date_to_show))
                errors_dates.update({plant.path: plant.date_to_show})
                continue

            if metadata.count({'path': plant.path}) <= 0:
                print('skip {} because no metadata for this plant'.format(plant.path))
                errors_metadata.append(plant.path)
                continue

            plant_paths.append(plant.path)

            if plant.path in mongo_plants_dict.keys():
                p = mongo_plant_update_is_date(plant.path, plant.name, plant.date_to_show.strftime('%Y-%m-%d'))  if plant.date_to_show  else mongo_plant_update_no_date(plant.path, plant.name)
                plants_collection.update_one(*p)
            else:
                p = mongo_plant_insert_is_date(plant.path, plant.name, plant.date_to_show.strftime('%Y-%m-%d') ) if plant.date_to_show else mongo_plant_insert_no_date(plant.path, plant.name)
                plants_collection.insert_one(p)

            if profiles_collection.find_one({'user_name': customer.login}):
                profiles_collection.update_one(*profiles_collection_update(customer.login, plant_paths, customer.is_admin))
            else:
                profiles_collection.insert_one(profiles_collection_insert(customer.login, plant_paths, customer.is_admin))

            all_pants_paths.append( plant_paths)

        profiles_collection.update_one(
             {'user_name': 'observer'},
            {'$set': {'plants': all_pants_paths}}
         )

        for path, date in errors_dates.items():
            print('There\'s no data_set for plant: {} and date: {}'.format(path, date))

        for path in errors_dates:
            print('There\'s no metadata for plant {}'.format(path))



def sync():
    import argparse
    parser = argparse.ArgumentParser()
    #parser.add_argument('--export_metadata', help='Also export metadata')
    parser.add_argument('--customers', nargs='*', help='Group customers to export')
    args = parser.parse_args()

    customers = args.customers if args.customers else False

    sync_users_plants(customers)

if __name__ == "__main__":
    t = time.time()
    sync()
    #print "%.3f" % (time.time()-t)

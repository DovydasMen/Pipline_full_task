from db_layer import MongoDb
from validator_schemes import nba_validator_scheme
import utility as utils
from sys import exit


def pipline_task():
    print("Hey!")
    print(
        "Your actions: \n1.Generate collection and aplly validation rule\n2.Create as much entries as you want.\n3.Task_1\n4.Task_2\n5.Task_3\n6.Task_4\n7.Task_5\n8.exit"
    )
    selection = int(input("Your selection: "))
    if selection == 1:
        db = MongoDb.create_database("0.0.0.0", "27017", "pipline")
        db.create_collection("NBA_prospects")
        db.apply_validation_rule("NBA_prospects", nba_validator_scheme)
        pipline_task()
    if selection == 2:
        template = utils.get_template(utils.get_field_counts())
        items_to_create = utils.get_entries_count()
        db = MongoDb.create_database("0.0.0.0", "27017", "pipline")
        while items_to_create > 0:
            db.insert_item("NBA_prospects", utils.random_values_generator(template))
            items_to_create = items_to_create - 1
        pipline_task()
    if selection == 3:
        db = MongoDb.create_database("0.0.0.0", "27017", "pipline")
        filter_criteria = {"health status": True}
        result = db.filter_documents("NBA_prospects", filter_criteria)
        if result == None:
            print("There was zero documents filtered!, Backing to main meniu!")
            pipline_task()
        else:
            for entries in result:
                print(
                    entries["name"], entries["surname"], entries["age"], entries["+/-"]
                )
        more_actions = input("Do you want to continue? y/n")
        if more_actions.lower() == "y":
            pipline_task()
        else:
            exit()

    if selection == 4:
        # db = MongoDb.create_database("0.0.0.0", "27017", "pipline")
        # filter_criteria = ""
        pass


if __name__ == "__main__":
    pipline_task()

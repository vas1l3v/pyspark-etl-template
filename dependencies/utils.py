from pyspark.sql.functions import udf


# custom udf functions used to apply operations on df columns
def convert_feet_to_cm(feet):
    tmp_feet = int(feet.split('\'')[0])
    tmp_inches = int(feet.split('\'')[1])
    foot_in_inches = 12
    inches_in_cm = 2.54
    cm = round((tmp_feet * foot_in_inches + tmp_inches) * inches_in_cm)
    return cm


def convert_lbs_to_kg(lbs):
    lbs = int(lbs.replace("lbs", ""))
    return round(lbs * 0.45359, 1)


# other util functions
def get_file_load_year(filepath):
    return filepath.split(".")[1]
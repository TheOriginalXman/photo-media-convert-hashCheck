from datetime import datetime

def parse_date(date_string):
    try:
        # First, try to parse the date string using the strptime() method
        date_object = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # If strptime() fails, try parsing the date string using the strptime() method with a different format
        date_object = datetime.strptime(date_string, "%Y-%m-%d")
    return date_object

def get_current_datetime_string():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return timestamp

# Example usage:
# date_string = "2022-03-31 12:34:56"
# date_object = parse_date(date_string)
# print(date_object)

# date_string = "2022-03-31"
# date_object = parse_date(date_string)
# print(date_object)

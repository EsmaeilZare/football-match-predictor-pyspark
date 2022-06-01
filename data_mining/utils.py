from datetime import datetime
import unicodedata


def clean_name(name):
    # delete numbers and special characters from player's name
    name_without_numbers = name.strip("#1234567890")
    # player name in European Soccer Dataset is in pure ascii so we should convert accented characters
    # convert accented characters to pure ascii
    ascii_name = unicodedata.normalize('NFD', name_without_numbers).encode('ascii', 'ignore')
    # remove redundant spaces
    cleaned_name = " ".join(filter(None, ascii_name.decode("ascii").split()))
    return cleaned_name


def clean_date(date_str: str):
    date = None
    date_str_list = date_str.split()
    try:
        date = datetime.strptime(" ".join(date_str_list[i] for i in range(3)), '%b %d, %Y')
    except Exception as e:
        print("error in saving birth_data ---> ", str(e))
    finally:
        return date


def clean_value(value_str):
    value = 0
    value_str = value_str.strip("€")
    if "Th." in value_str:
        value = float(value_str.replace("Th.", "")) * 1000
    elif "m" in value_str:
        value = float(value_str.replace("m", "")) * 1000000
    return int(value)





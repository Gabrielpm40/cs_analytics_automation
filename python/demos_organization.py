from pandas import DataFrame, Timestamp, DatetimeIndex, read_parquet, concat
from pandas.core.frame import DataFrame as DataFrame_type
from glob import glob
from pathlib import Path
from os import listdir, mkdir, remove, rename
from os.path import basename, isdir, getctime
from json import load, dump
from shutil import copy
from patoolib import extract_archive
from awpy import Demo

teams_path = Path('../JSON/teams.json').resolve().__str__()
maps_path = Path('../JSON/maps.json').resolve().__str__()
demo_attrs_path = Path('../JSON/demo_attrs.json').resolve().__str__()



teams = load(open(teams_path))
maps = load(open(maps_path))
demo_attrs = load(open(demo_attrs_path))




def update_teams_json()->None:
    with open(teams_path) as file:
        teams = load(file)

def add_team(team: str)->None:
    if team in teams:
        return None
    teams.append(team)
    with open(teams_path, "w") as outfile:
        dump(teams, outfile, indent=4)
    update_teams_json()

def remove_team(team: str)->None:
    teams.remove(team)
    with open(teams_path, "w") as outfile:
        dump(teams, outfile, indent=4)
    update_teams_json()

def correct_team_name(wrong_name:str, correct_name:str)->None:
    if correct_name in teams:
        remove_team(wrong_name)
        return None
    remove_team(wrong_name)
    add_team(correct_name)









def update_maps_json()->None:
    with open(maps_path) as file:
        maps = load(file)

def add_map(map: str)->None:
    if map in maps:
        return None
    maps.append(map)
    with open(maps_path, "w") as outfile:
        dump(maps, outfile, indent=4)
    update_maps_json()

def remove_map(map: str)->None:
    maps.remove(map)
    with open(maps_path, "w") as outfile:
        dump(maps, outfile, indent=4)
    update_maps_json()


def correct_map_name(wrong_name:str, correct_name:str)->None:
    if correct_name in maps:
        remove_map(wrong_name)
        return None
    remove_map(wrong_name)
    add_map(correct_name)













def correct_team_name_in_files(wrong_team_name: str, right_team_name: str)->None:
    """# Rename all the demo files correcting a wrong team name.
    ## Correct it based on the teams list from teams.json"""
    for demo in listdir('../demos'):
        if wrong_team_name in demo:
            rename(Path(f'../demos/{demo}').resolve().__str__(),
                Path(f'../demos/{demo}').resolve().__str__().replace(wrong_team_name, right_team_name))
            

def organize_demos()->None:
    """# Organize all demos for the respective teams.
        ## Don't forget to mantain the demos folder with only demo files, which names strings (no matter the order) must contain the teams and the map played. """
    for demo in listdir('../demos'):
        team_team_map_tuple = tuple(_ for _ in teams if _ in demo) + tuple(__ for __ in maps if __ in demo)
        if len(team_team_map_tuple) == 1:
            print(f'You gotta add a team from {demo} to teams.json')
            break
        if len(team_team_map_tuple) == 2:
            copy(Path(f'../demos/{demo}').resolve().__str__(), Path(f'../teams/{team_team_map_tuple[0]}/{team_team_map_tuple[-1]}/{demo}').resolve().__str__())
            with open('teams_to_add/teams_to_add.txt', 'w') as f:
                f.writelines(f'{demo}\n')
                f.close()
            break
        if Path(f'../teams/{team_team_map_tuple[0]}/{team_team_map_tuple[-1]}/{demo}').is_file() and Path(f'../teams/{team_team_map_tuple[1]}/{team_team_map_tuple[-1]}/{demo}').is_file():
            remove(f'../demos/{demo}')
            continue
        copy(Path(f'../demos/{demo}').resolve().__str__(), Path(f'../teams/{team_team_map_tuple[0]}/{team_team_map_tuple[-1]}/{demo}').resolve().__str__())
        copy(Path(f'../demos/{demo}').resolve().__str__(), Path(f'../teams/{team_team_map_tuple[1]}/{team_team_map_tuple[-1]}/{demo}').resolve().__str__())
        remove(f'../demos/{demo}')

def create_teams_folders()->None:
    """# Create teams folders based on the list teams.json"""
    for team in teams:
        if team in listdir('../teams'):
            continue
        else:
            mkdir(f'../teams/{team}')
            for map in maps:
                mkdir(f'../teams/{team}/{map}')

def add_maps_to_teams_folders()->None:
    """# Add the maps from list maps.json to the teams folders"""
    for team in teams:
        for map in maps:
            if map in listdir(f'../teams/{team}'):
                continue
            else:
                mkdir(f'../teams/{team}/{map}')

def extract_demos()->None:
    """# Extract all demos from .rar"""
    for demo in glob('../demos/*.rar*'):
        extract_archive(Path(demo).resolve().__str__(), outdir=Path(f'../demos').resolve().__str__())
        remove(Path(demo).resolve().__str__())

    

def make_dataframes(team: str, map: str)->None:
    """# Make all the dataframes from demos"""
    for demo in glob(f'../teams/{team}/{map}/*.dem*'):
        teams_in_file = list(_ for _ in teams if _ in basename(Path(demo)))
        teams_in_file.remove(team)
        arq = basename(Path(demo)).replace('.dem', '-folder')
        date = Timestamp.fromtimestamp(getctime(Path(demo).resolve().__str__())).strftime('%d-%m-%y-%H-%M')
        if isdir(Path(f'../teams/{team}/{map}/{arq}')) and isdir(Path(f'../teams/{teams_in_file[0]}/{map}/{arq}')):
            continue
        mkdir(f'../teams/{team}/{map}/{arq}')
        mkdir(f'../teams/{teams_in_file[0]}/{map}/{arq}')
        demo = Demo(demo)
        for attr in demo_attrs:
            mkdir(f'../teams/{team}/{map}/{arq}/{attr}')
            mkdir(f'../teams/{teams_in_file[0]}/{map}/{arq}/{attr}')
            demo.__dict__[attr].to_parquet(Path(f'../teams/{team}/{map}/{arq}/{attr}/{date}.parquet'))
            copy(Path(f'../teams/{team}/{map}/{arq}/{attr}/{date}.parquet').resolve().__str__(),
                      Path(f'../teams/{teams_in_file[0]}/{map}/{arq}/{attr}/{date}.parquet').resolve().__str__() )

def replace_string_by_index(string: str, index: int, new_char: str)->str:
    return string[:index] + string[index:].replace(string[index], new_char, 1)

def get_attr_by_team(team: str, map:str, attr:str)->DataFrame_type:
    """# Get the full dataframe based on the already parsed demos"""
    dfr = DataFrame({})
    for demo in glob(f'../teams/{team}/{map}/*-folder*'):
        demo = basename(demo)
        file = listdir(f'../teams/{team}/{map}/{demo}/{attr}')[0]
        date = replace_string_by_index(file.replace('.parquet', ''), index=-3, new_char=':')
        if dfr.empty:
            dfr = read_parquet(Path(f'../teams/{team}/{map}/{demo}/{attr}/{file}').resolve().__str__())
            dfr.set_index(DatetimeIndex([Timestamp(date)] * len(dfr), name='match'), inplace=True)
        dfr1 = read_parquet(Path(f'../teams/{team}/{map}/{demo}/{attr}/{file}').resolve().__str__())
        dfr1.set_index(DatetimeIndex([Timestamp(date)] * len(dfr1), name='match'), inplace=True)
        dfr = concat([dfr, dfr1])
    return dfr





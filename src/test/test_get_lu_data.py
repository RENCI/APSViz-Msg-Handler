# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Tests to interrogate database operations

"""
from src.common.pg_impl import PGImplementation


def test_db_connection_creation():
    """
    Tests the creation and usage of the db utils DB multi-connect class

    :return:
    """
    # specify the DB to gain connectivity to
    db_name: tuple = ('apsviz',)

    # create a DB connection object
    db_info = PGImplementation(db_name)

    # check the object returned
    assert len(db_info.dbs) == len(db_name)

    # make a db request
    ret_val = db_info.exec_sql('apsviz', 'SELECT version()')

    # check the data returned
    assert ret_val.startswith('PostgreSQL')


def test_get_lu_id():
    """
    Tests the retrieval of LU data for sites

    :return:
    """
    # create the DB object
    db_names: tuple = ('apsviz',)

    # define and init the object that will handle DB operations
    db_info: PGImplementation = PGImplementation(db_names, _logger=None)

    # get a site id item
    site_id = db_info.get_lu_id('RENCI', 'site')

    # the renci site has an id of 0
    assert site_id == 0

    # get a site id item
    site_id = db_info.get_lu_id('TWI', 'site')

    # the twi site has an id of 12
    assert site_id == 12


def test_get_site_ids():
    """
    Tests the retrieval of a list of site IDs

    :return:
    """
    # create the DB object
    db_names: tuple = ('apsviz',)

    # define and init the object that will handle DB operations
    db_info: PGImplementation = PGImplementation(db_names, _logger=None)

    # get the site ids
    site_ids: list = db_info.get_site_ids()

    # check the result
    assert len(db_info.legacy_constants['site']) == len(site_ids)


def test_load_lu_data():
    """
    Tests the retrieval of lookup data

    :return:
    """
    # create the DB object
    db_names: tuple = ('apsviz',)

    # define and init the object that will handle DB operations
    db_info: PGImplementation = PGImplementation(db_names, _logger=None)

    # define the expected LU constant lookups
    exp_site_lu = {'RENCI': 0, 'TACC': 1, 'LSU': 2, 'UCF': 3, 'George Mason': 4, 'Penguin': 5, 'LONI': 6, 'Seahorse': 7, 'QB2': 8, 'CCT': 9,
                   'PSC': 10, 'UGA': 11, 'TWI': 12}
    exp_event_type_lu = {'RSTR': 0, 'PRE1': 1, 'NOWC': 2, 'PRE2': 3, 'FORE': 4, 'POST': 5, 'REND': 6, 'STRT': 7, 'HIND': 8, 'EXIT': 9, 'FSTR': 10,
                         'FEND': 11, 'PNOW': 12}
    exp_state_type_lu = {'INIT': 0, 'RUNN': 1, 'PEND': 2, 'FAIL': 3, 'WARN': 4, 'IDLE': 5, 'CMPL': 6, 'NONE': 7, 'WAIT': 8, 'EXIT': 9, 'STALLED': 10}
    exp_instance_state_type_lu = {'INIT': 0, 'RUNN': 1, 'PEND': 2, 'FAIL': 3, 'WARN': 4, 'IDLE': 5, 'CMPL': 6, 'NONE': 7, 'WAIT': 8, 'EXIT': 9,
                                  'STALLED': 10}

    # create a dict that can be used to compare with the DB output
    expected_lu_data: dict = {'site': exp_site_lu, 'event_type': exp_event_type_lu, 'state_type': exp_state_type_lu,
                              'instance_state_type': exp_instance_state_type_lu}

    # create a list of target lu tables
    lu_tables = ['site_lu', 'event_type_lu', 'state_type_lu', 'instance_state_type_lu']

    # init the lu_data storage
    lu_data: dict = {}

    # make the call to get the data
    for lu_item in lu_tables:
        lu_data.update({lu_item.removesuffix('_lu'): db_info.get_lu_items(lu_item)})

    assert lu_data == expected_lu_data

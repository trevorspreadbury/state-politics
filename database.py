import os
from typing import DefaultDict
import pandas as pd
from psycopg2 import sql

from postgres import Database
from constants import state_abbreviations, state_names
import csv

csv_type_to_schema = {
    "bill_abstracts": 
        """CREATE TABLE IF NOT EXISTS bill_abstracts (
            id UUID PRIMARY KEY,
            bill_id UUID REFERENCES bills,
            abstract TEXT,
            note TEXT
        );
        """,
    "bill_actions":
        """CREATE TABLE IF NOT EXISTS bill_actions (
            id UUID PRIMARY KEY,
            bill_id UUID REFERENCES bills,
            organization_id UUID,
            description TEXT,
            date DATE, 
            classification bill_action_classification[],
            bill_order SMALLINT
        );
        """,
    "bills":
        """CREATE TABLE IF NOT EXISTS bills (
            id UUID PRIMARY KEY,
            identifier VARCHAR(10),
            title VARCHAR(100),
            classification bill_classification[],
            subject VARCHAR(10),
            session_identifier VARCHAR(13),
            jurisdiction VARCHAR(20),
            organization_classification organization_classification_enum
        );
        """,
    "bill_document_links":
        """CREATE TABLE IF NOT EXISTS bill_document_links (
            id UUID PRIMARY KEY,
            media_type VARCHAR(10),
            url TEXT,
            document_id UUID REFERENCES bill_documents
        );
        """,
    "bill_documents":
        """CREATE TABLE IF NOT EXISTS bill_documents (
            id UUID PRIMARY KEY,
            bill_id UUID REFERENCES bills,
            note VARCHAR(50),
            date DATE,
            classification VARCHAR(20),
            extras VARCHAR(20)
        );
        """,
    "bill_sources":
        """CREATE TABLE IF NOT EXISTS bill_sources (
            id UUID PRIMARY KEY,
            note VARCHAR(20),
            url text,
            bill_id UUID REFERENCES bills
        );
        """,
    "bill_sponsorships":
        """CREATE TABLE IF NOT EXISTS bill_sponsorships (
            id UUID PRIMARY KEY,
            name VARCHAR(100),
            entity_type VARCHAR(16),
            organization_id UUID,
            person_id UUID,
            bill_id UUID REFERENCES bills,
            primary_sponsor BOOLEAN,
            classification bill_sponsorship_classification
        );
        """,
    "bill_version_links":
        """CREATE TABLE IF NOT EXISTS bill_version_links (
            id UUID PRIMARY KEY,
            media_type VARCHAR(20),
            url TEXT,
            version_id UUID REFERENCES bill_versions
        );
        """,
    "bill_versions":
        """CREATE TABLE IF NOT EXISTS bill_versions (
            id UUID PRIMARY KEY,
            bill_id UUID REFERENCES bills,
            note VARCHAR(100),
            date DATE,
            classification VARCHAR(20),
            extras VARCHAR(20)
        );
        """,
    "vote_counts":
        """CREATE TABLE IF NOT EXISTS vote_counts (
            id UUID PRIMARY KEY,
            vote_event_id UUID REFERENCES votes,
            option vote_option,
            value SMALLINT
        );
        """,
    "vote_people":
        """CREATE TABLE IF NOT EXISTS vote_people (
            id UUID PRIMARY KEY,
            vote_event_id UUID REFERENCES votes,
            option vote_option,
            voter_name VARCHAR(50),
            voter_id UUID,
            note VARCHAR(50)
        );
        """,
    "vote_sources":
        """CREATE TABLE IF NOT EXISTS vote_sources (
            id UUID PRIMARY KEY,
            url VARCHAR(100),
            note VARCHAR(100),
            vote_event_id UUID REFERENCES votes
        );
        """,
    "votes":
        """CREATE TABLE IF NOT EXISTS votes (
            id UUID PRIMARY KEY,
            identifier VARCHAR(10),
            motion_text VARCHAR(50),
            motion_classification bill_action_classification[],
            start_date DATE,
            result result_enum,
            organization_id UUID,
            bill_id UUID REFERENCES bills,
            bill_action_id UUID REFERENCES bill_actions,
            jurisdiction VARCHAR(16),
            session_identifier VARCHAR(16)
        );
        """,
    "people":
        """CREATE TABLE IF NOT EXISTS people (
            id UUID PRIMARY KEY,
            name VARCHAR(50),
            current_party party,
            current_district SMALLINT,
            current_chamber organization_classification,
            given_name VARCHAR(25),
            family_name VARCHAR(25),
            gender VARCHAR(10),
            email VARCHAR(50),
            biography TEXT,
            birth_date DATE,
            death_date DATE,
            image VARCHAR(100),
            links TEXT,
            sources TEXT,
            capitol_address VARCHAR(100),
            capitol_voice VARCHAR(50),
            capitol_fax VARCHAR(50),
            district_address VARCHAR(100),
            district_voice VARCHAR(50),
            district_fax VARCHAR(50),
            twitter VARCHAR(50),
            youtube VARCHAR(50),
            instagram VARCHAR(50),
            facebook VARCHAR(50)
        );
        """,
}

table_order = [
    "bills", "people", "bill_documents", "bill_actions", "bill_abstracts", "bill_sponsorships",
    "bill_sources", "bill_versions", "bill_version_links", "votes",
    "vote_sources", "vote_counts", "vote_people", "bill_document_links"]

def to_set(string):
    return {string}

def populate_database(data_file_path):
    data_file = data_file_path.split("/")[-1]
    state, session, special, csv_type = tokenize_data_file(data_file)

    ## Remove prefixes from foreign key uuids
    with open(data_file_path, "r") as infile:
        table = pd.read_csv(infile, dtype=str)
        for column_name in table.columns:
            ## column names that end in '_id' are uuids prefixed with 
            ## foreign key column name. We want to remove the prefix
            ## so Postgres accepts the uuid
            if column_name[-2:] == "id":
                ## find the prefix (which always ends in /, and remove it)
                try:
                    table[column_name] = table[column_name].str.split("/").str[-1]
                except Exception as e:
                    print(e)
            elif "classification" in column_name:
                table[column_name] = table[column_name].str.replace("[", "{", regex=False)
                table[column_name] = table[column_name].str.replace("]", "}", regex=False)
                table[column_name] = table[column_name].str.replace("'", "", regex=False)

    with open(data_file_path, "w") as cleaned_file:
        table.to_csv(cleaned_file, index=False)

    ## Connect to postgres database for given state
    with Database(state) as db:
        # get create table command template for csv type. We will create
        # a temporary table to coppy into and then upsert data into the
        # real table. This allows us to sidestep the lack of a postgres
        # COPY ... ON CONFLICT command
        temp_name = csv_type + "_temp"
        create_table = csv_type_to_schema[csv_type]
        #db.query(create_table.format("TEMPORARY", temp_name))
        db.query(create_table)
        db.query(sql.SQL("CREATE TEMPORARY TABLE {} (LIKE {} INCLUDING ALL) ON COMMIT DROP;").format(sql.Identifier(temp_name), sql.Identifier(csv_type)))
        db.copy_from(data_file_path, temp_name)
        db.query(sql.SQL("INSERT INTO {} SELECT * FROM {} ON CONFLICT DO NOTHING;").format(sql.Identifier(csv_type), sql.Identifier(temp_name)))

        db.commit()
    return


def initialize_database(state):
    """ ensure proper tables exist in desired database """
    bill_action_classification = (
        """
        DO $$ BEGIN
            CREATE  TYPE bill_action_classification AS ENUM 
            (
                'filing', 'introduction', 'reading-1', 'reading-2', 'reading-3',
                'referral-committee', 'committee-passage', 'committee-failure',
                'committee-passage-favorable', 'amendment-passage',
                'amendment-failure', 'amendment-introduction',
                'executive-signature', 'executive-receipt', 'executive-veto',
                'executive-veto-line-item', 'veto-override-passage',
                'withdrawal', 'passage', 'failure', 'became-law'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """
    )
    organization_classification = (
        """
        DO $$ BEGIN
            CREATE TYPE organization_classification_enum AS ENUM 
            ('upper', 'lower');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """
    )
    bill_classification = (
        """
        DO $$ BEGIN
            CREATE TYPE bill_classification AS ENUM 
            (
                'resolution', 'bill', 'joint resolution', 
                'constitutional amendment', 'appointment'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """
    )
    bill_sponsorship_classification = (
        """
        DO $$ BEGIN
            CREATE TYPE bill_sponsorship_classification AS ENUM 
            (
                'primary', 'cosponsor'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """
    )
    vote_option = (
        """
        DO $$ BEGIN
            CREATE TYPE vote_option AS ENUM 
            (
                'yes', 'no', 'not voting', 'absent', 'excused', 'other'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """
    )
    result_enum = (
        """
        DO $$ BEGIN
            CREATE TYPE result_enum AS ENUM 
            (
                'pass', 'fail'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """
    )
    party = (
        """
        DO $$ BEGIN
            CREATE TYPE party AS ENUM 
            (
                'Republican', 'Democratic', 'Libertarian', 'Green', 'Independent'
            );
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        """
    )

    with Database(state) as db:
        db.query(bill_action_classification)
        db.query(organization_classification)
        db.query(bill_classification)
        db.query(bill_sponsorship_classification)
        db.query(result_enum)
        db.query(vote_option)
        db.query(party)
        db.commit()


def tokenize_data_file(data_file):
    """ separate openstates bulk data csv file name into tokens """
    tokens = data_file.split(".")[0].split("/")[-1].split("_")
    csv_type = "_".join(tokens[2:])
    state = state_names[tokens[0]].lower()
    session = tokens[1]
    if "-" in session:
        special = True
        session = session.split("-")[0]
    else:
        special = False
    session_number = int(session[:-2])
    return state, session_number, special, csv_type

def insert_openstates_into_postgres(state):
    """ inserts data from openstates into postgres database for state """
    initialize_database(state)
    state_abbreviation = state_abbreviations[state.capitalize()]
    ## start by filling legislator table
    legislator_file = state_abbreviation.upper() + "_00NA_people.csv"
    data_file_path = os.path.join(state_abbreviation, legislator_file)
    populate_database(data_file_path)

    for session in os.listdir(state_abbreviation):
        session_directory = os.path.join(state_abbreviation, session)
        for table in table_order:
            csv_name = (
                "_".join([state_abbreviation.upper(), session, table])
                + ".csv"
            )
            data_file_path = os.path.join(session_directory, csv_name)
            if os.path.isfile(data_file_path):
                print(csv_name)
                populate_database(data_file_path)
            else:
                print("NOT A FILE: ", csv_name)
        # for data_file in os.listdir(session_directory):
        #     data_file_path = os.path.join(session_directory, data_file)
        #     print(data_file)
        #     populate_database(data_file_path)

if __name__ == "__main__":
    insert_openstates_into_postgres("illinois")
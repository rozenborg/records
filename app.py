import os
from datetime import datetime
import pandas as pd
import streamlit as st
import io # Needed for file uploads
import shutil # For file operations
import json # For version control
from typing import Union # Import Union

###############################################################################
# Version Control & Migration
###############################################################################
APP_VERSION = "1.2.2"  # Current app version
VERSION_FILE = "version.json"  # Stores schema version info
BACKUP_DIR = "backups"  # Directory for data backups

def get_current_schema_version():
    """Get the current schema version from the version file."""
    if os.path.exists(VERSION_FILE):
        try:
            with open(VERSION_FILE, 'r') as f:
                version_data = json.load(f)
                return version_data.get('schema_version', '0.0.0')
        except (json.JSONDecodeError, IOError):
            return '0.0.0'
    return '0.0.0'

def update_schema_version(new_version):
    """Update the schema version in the version file."""
    version_data = {'schema_version': new_version, 'updated_at': datetime.now().isoformat()}
    with open(VERSION_FILE, 'w') as f:
        json.dump(version_data, f)

def create_backup():
    """Create a timestamped backup of all data files."""
    if not os.path.exists(DATA_DIR):
        return False
    
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = os.path.join(BACKUP_DIR, f"backup_{timestamp}")
    os.makedirs(backup_path, exist_ok=True)
    
    # Copy all CSV files to the backup directory
    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith('.csv'):
            source_path = os.path.join(DATA_DIR, file_name)
            dest_path = os.path.join(backup_path, file_name)
            shutil.copy2(source_path, dest_path)
    
    return backup_path

def run_migrations(from_version, to_version):
    """Run database migrations from one version to another."""
    # Define migrations as a dictionary with from_version -> to_version keys and migration functions as values
    migrations = {
        '0.0.0->1.0.0': migrate_from_0_to_1,
        '1.0.0->1.1.0': migrate_from_1_0_0_to_1_1_0,
        '1.1.0->1.2.0': migrate_from_1_1_0_to_1_2_0, # New participant data structure
        '1.2.0->1.2.1': migrate_from_1_2_0_to_1_2_1,
        '1.2.1->1.2.2': migrate_from_1_2_1_to_1_2_2
    }
    
    # Execute appropriate migration based on version transition
    migration_key = f'{from_version}->{to_version}'
    if migration_key in migrations:
        # Create backup before running migrations
        backup_path = create_backup()
        st.info(f"Created backup at {backup_path} before applying migrations")
        
        # Run the migration
        migrations[migration_key]()
        update_schema_version(to_version)
        st.success(f"Successfully migrated from v{from_version} to v{to_version}")
        return True
    
    # If we're on a new installation (version 0.0.0), just update the version
    if from_version == '0.0.0':
        update_schema_version(to_version)
        return True
        
    return False

# Example migration function
def migrate_from_0_to_1():
    """Migration from version 0.0.0 to 1.0.0."""
    try:
        # Add "Last Updated" column to participants.csv if it doesn't exist
        if os.path.exists(os.path.join(DATA_DIR, "participants.csv")):
            df = pd.read_csv(os.path.join(DATA_DIR, "participants.csv"))
            if "Last Updated" not in df.columns:
                df["Last Updated"] = ""
                df.to_csv(os.path.join(DATA_DIR, "participants.csv"), index=False)
                st.info("Added 'Last Updated' column to participants.csv")
        
        # Add "Hosted" field to participants.csv if it doesn't exist
        if os.path.exists(os.path.join(DATA_DIR, "participants.csv")):
            df = pd.read_csv(os.path.join(DATA_DIR, "participants.csv"))
            if "Hosted" not in df.columns:
                df["Hosted"] = "No"  # Default all existing records to "No"
                df.to_csv(os.path.join(DATA_DIR, "participants.csv"), index=False)
                st.info("Added 'Hosted' column to participants.csv")
        
        # Add "Hosted" field to events.csv if it doesn't exist
        if os.path.exists(os.path.join(DATA_DIR, "events.csv")):
            df = pd.read_csv(os.path.join(DATA_DIR, "events.csv"))
            if "Hosted" not in df.columns:
                df["Hosted"] = ""  # Empty list of hosted IDs
                df.to_csv(os.path.join(DATA_DIR, "events.csv"), index=False)
                st.info("Added 'Hosted' column to events.csv")
    except Exception as e:
        st.error(f"Migration failed: {str(e)}")
        raise

# Migration for version 1.1.0
def migrate_from_1_0_0_to_1_1_0():
    """Migration from version 1.0.0 to 1.1.0 for cohorts.
    - Renames 'Nominees' to 'Nominated' in cohorts.csv.
    - Renames 'Participants' to 'Joined' in cohorts.csv.
    - Adds 'Invited' column to cohorts.csv.
    """
    try:
        cohorts_path = os.path.join(DATA_DIR, "cohorts.csv")
        if os.path.exists(cohorts_path):
            df = pd.read_csv(cohorts_path)
            changes_made = False
            # Rename Nominees to Nominated
            if "Nominees" in df.columns:
                if "Nominated" not in df.columns:
                    df = df.rename(columns={"Nominees": "Nominated"})
                    st.info("Renamed 'Nominees' to 'Nominated' in cohorts.csv")
                    changes_made = True
                else: # Both exist, drop old Nominees if it's different or just ensure it's gone
                    if not df["Nominees"].equals(df["Nominated"]):
                         st.warning("Both 'Nominees' and 'Nominated' columns found with different data. Prioritizing 'Nominated'. Old 'Nominees' data might be lost if not manually merged.")
                    df = df.drop(columns=["Nominees"])
                    st.info("Dropped legacy 'Nominees' column in cohorts.csv as 'Nominated' exists.")
                    changes_made = True
            
            # Rename Participants to Joined
            if "Participants" in df.columns:
                if "Joined" not in df.columns:
                    df = df.rename(columns={"Participants": "Joined"})
                    st.info("Renamed 'Participants' to 'Joined' in cohorts.csv")
                    changes_made = True
                else: # Both exist, drop old Participants
                    if not df["Participants"].equals(df["Joined"]):
                        st.warning("Both 'Participants' and 'Joined' columns found with different data. Prioritizing 'Joined'. Old 'Participants' data might be lost if not manually merged.")
                    df = df.drop(columns=["Participants"])
                    st.info("Dropped legacy 'Participants' column in cohorts.csv as 'Joined' exists.")
                    changes_made = True

            # Add Invited column
            if "Invited" not in df.columns:
                df["Invited"] = ""  # Default to empty string
                st.info("Added 'Invited' column to cohorts.csv")
                changes_made = True
            
            if changes_made:
                df.to_csv(cohorts_path, index=False)
                st.info("cohorts.csv updated by migration to v1.1.0")

    except Exception as e:
        st.error(f"Migration to 1.1.0 (cohorts) failed: {str(e)}")
        raise

# Migration for version 1.2.0: Restructures participants.csv
def migrate_from_1_1_0_to_1_2_0():
    """Migration from v1.1.0 to v1.2.0:
    Restructures participants.csv to be one row per employee, aggregating event
    and cohort data into comma-separated lists.
    """
    st.info("Starting migration to v1.2.0 for participants data structure...")
    try:
        employees_df = load_table("employees") # Load with new load_table
        if employees_df.empty:
            st.warning("Employees table is empty. Skipping participants migration as there's no base data.")
            # Create an empty participants.csv with the new schema if it doesn't exist or is old
            participants_path = _path_for("participants")
            _, new_participant_cols = FILES["participants"]
            # Check if participants.csv exists and if its columns match the new schema
            create_new_empty = True
            if os.path.exists(participants_path):
                try:
                    current_participant_df_cols = pd.read_csv(participants_path, nrows=0).columns.tolist()
                    if set(current_participant_df_cols) == set(new_participant_cols):
                        create_new_empty = False # Already new schema
                except Exception:
                    pass # Will be overwritten
            
            if create_new_empty:
                pd.DataFrame(columns=new_participant_cols).to_csv(participants_path, index=False)
                st.info("Created empty participants.csv with new schema.")
            return

        new_participants_cols = FILES["participants"][1]
        # Initialize new participants DataFrame from employees
        new_participants_df = employees_df[["Standard ID", "Email"]].copy()
        for col in new_participants_cols:
            if col not in new_participants_df.columns:
                new_participants_df[col] = ""

        new_participants_df["Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Prepare for aggregation (use sets to handle duplicates, then join)
        # Initialize dictionary to hold sets of IDs/names for each employee
        agg_data = {emp_id: {
            "Events Registered": set(),
            "Events Participated": set(),
            "Events Hosted": set(),
            "Cohorts Nominated": set(),
            "Cohorts Invited": set(),
            "Cohorts Joined": set()
        } for emp_id in new_participants_df["Standard ID"]}

        # 1. Process old participants.csv (if it exists and is in the old format)
        old_participants_path = os.path.join(DATA_DIR, "participants.csv")
        if os.path.exists(old_participants_path):
            try:
                old_participants_df = pd.read_csv(old_participants_path, dtype=str, na_filter=False).fillna("")
                # Check if it's the old format by looking for "Event ID" and "Registered" etc.
                if "Event ID" in old_participants_df.columns and "Registered" in old_participants_df.columns:
                    st.info("Processing data from existing (old format) participants.csv...")
                    for _, row in old_participants_df.iterrows():
                        emp_id = str(row["Standard ID"])
                        event_id = str(row["Event ID"])
                        if emp_id in agg_data: # Ensure employee exists in our master list
                            if str(row.get("Registered", "")).lower() == "yes":
                                agg_data[emp_id]["Events Registered"].add(event_id)
                            if str(row.get("Participated", "")).lower() == "yes":
                                agg_data[emp_id]["Events Participated"].add(event_id)
                            if str(row.get("Hosted", "")).lower() == "yes":
                                agg_data[emp_id]["Events Hosted"].add(event_id)
                    st.info("Completed processing old participants.csv data.")
                else:
                    st.info("Existing participants.csv does not seem to be old format. Will ensure schema matches new format.")
            except Exception as e:
                st.warning(f"Could not process old participants.csv: {e}. It might be empty or corrupted. Will proceed assuming no prior participant event details.")

        # 2. Process events.csv to ensure all event links are captured
        events_path = _path_for("events")
        if os.path.exists(events_path):
            try:
                events_df = pd.read_csv(events_path, dtype=str, na_filter=False).fillna("")
                st.info("Processing data from events.csv...")
                for _, event_row in events_df.iterrows():
                    event_id = str(event_row["Event ID"])
                    for emp_id_str in str(event_row.get("Registrations", "")).split(','):
                        emp_id = emp_id_str.strip()
                        if emp_id and emp_id in agg_data:
                            agg_data[emp_id]["Events Registered"].add(event_id)
                    for emp_id_str in str(event_row.get("Participants", "")).split(','):
                        emp_id = emp_id_str.strip()
                        if emp_id and emp_id in agg_data:
                            agg_data[emp_id]["Events Participated"].add(event_id)
                    for emp_id_str in str(event_row.get("Hosted", "")).split(','):
                        emp_id = emp_id_str.strip()
                        if emp_id and emp_id in agg_data:
                            agg_data[emp_id]["Events Hosted"].add(event_id)
                st.info("Completed processing events.csv data.")
            except Exception as e:
                st.error(f"Failed to process events.csv during migration: {e}")
                raise

        # 3. Process cohorts.csv
        cohorts_path = _path_for("cohorts")
        if os.path.exists(cohorts_path):
            try:
                cohorts_df = pd.read_csv(cohorts_path, dtype=str, na_filter=False).fillna("")
                st.info("Processing data from cohorts.csv...")
                for _, cohort_row in cohorts_df.iterrows():
                    cohort_name = str(cohort_row["Name"])
                    for emp_id_str in str(cohort_row.get("Nominated", "")).split(','): # Uses "Nominated" from v1.1.0
                        emp_id = emp_id_str.strip()
                        if emp_id and emp_id in agg_data:
                            agg_data[emp_id]["Cohorts Nominated"].add(cohort_name)
                    for emp_id_str in str(cohort_row.get("Invited", "")).split(','):
                        emp_id = emp_id_str.strip()
                        if emp_id and emp_id in agg_data:
                            agg_data[emp_id]["Cohorts Invited"].add(cohort_name)
                    for emp_id_str in str(cohort_row.get("Joined", "")).split(','): # Uses "Joined" from v1.1.0
                        emp_id = emp_id_str.strip()
                        if emp_id and emp_id in agg_data:
                            agg_data[emp_id]["Cohorts Joined"].add(cohort_name)
                st.info("Completed processing cohorts.csv data.")
            except Exception as e:
                st.error(f"Failed to process cohorts.csv during migration: {e}")
                raise
        
        # Populate the new_participants_df with aggregated data
        st.info("Aggregating processed data into new participants structure...")
        for emp_id, data_sets in agg_data.items():
            idx = new_participants_df[new_participants_df["Standard ID"] == emp_id].index
            if not idx.empty:
                target_idx = idx[0]
                new_participants_df.loc[target_idx, "Events Registered"] = ",".join(sorted(list(filter(None, data_sets["Events Registered"]))))
                new_participants_df.loc[target_idx, "Events Participated"] = ",".join(sorted(list(filter(None, data_sets["Events Participated"]))))
                new_participants_df.loc[target_idx, "Events Hosted"] = ",".join(sorted(list(filter(None, data_sets["Events Hosted"]))))
                new_participants_df.loc[target_idx, "Cohorts Nominated"] = ",".join(sorted(list(filter(None, data_sets["Cohorts Nominated"]))))
                new_participants_df.loc[target_idx, "Cohorts Invited"] = ",".join(sorted(list(filter(None, data_sets["Cohorts Invited"]))))
                new_participants_df.loc[target_idx, "Cohorts Joined"] = ",".join(sorted(list(filter(None, data_sets["Cohorts Joined"]))))
                # "Nominated By" remains empty for now as this data isn't tracked previously
                new_participants_df.loc[target_idx, "Nominated By"] = "" 
        
        # Ensure all columns are present and fill NaNs just in case
        for col in new_participants_cols:
            if col not in new_participants_df.columns:
                new_participants_df[col] = ""
        new_participants_df = new_participants_df.fillna("")

        # Save the new participants.csv
        final_participants_path = _path_for("participants")
        new_participants_df[new_participants_cols].to_csv(final_participants_path, index=False) # Enforce column order
        st.success("Successfully migrated participants data to new structure (v1.2.0).")
        load_table.clear() # Clear cache

    except Exception as e:
        st.error(f"Migration to v1.2.0 (participants restructure) failed: {str(e)}")
        st.error("Your data might be in an inconsistent state. Consider restoring from a backup if issues persist.")
        raise

# Migration for version 1.2.1: Adds 'On General Waitlist' to participants
def migrate_from_1_2_0_to_1_2_1():
    """Migration from v1.2.0 to v1.2.1:
    - Removes 'Waitlisted' column from events.csv if it was added by a previous incorrect migration attempt for 1.2.1.
    - Adds 'On General Waitlist' (Yes/No string) to participants.csv.
    - Removes 'Events Waitlisted' column from participants.csv if it was added by a previous incorrect migration attempt for 1.2.1.
    """
    st.info("Starting migration to v1.2.1 for 'On General Waitlist' feature...")
    try:
        # Attempt to remove 'Waitlisted' from events.csv (if it exists from previous incorrect migration)
        events_path = _path_for("events")
        if os.path.exists(events_path):
            try:
                events_df = pd.read_csv(events_path, dtype=str, na_filter=False).fillna("")
                if "Waitlisted" in events_df.columns:
                    events_df = events_df.drop(columns=["Waitlisted"])
                    events_df.to_csv(events_path, index=False)
                    st.info("Removed legacy 'Waitlisted' column from events.csv.")
            except Exception as e:
                st.warning(f"Could not process events.csv to remove old Waitlisted column: {e}")

        # Add 'On General Waitlist' to participants.csv and remove old 'Events Waitlisted'
        participants_path = _path_for("participants")
        if os.path.exists(participants_path):
            participants_df = pd.read_csv(participants_path, dtype=str, na_filter=False).fillna("")
            made_changes_participants = False
            if "Events Waitlisted" in participants_df.columns:
                participants_df = participants_df.drop(columns=["Events Waitlisted"])
                st.info("Removed legacy 'Events Waitlisted' column from participants.csv.")
                made_changes_participants = True
            
            if "On General Waitlist" not in participants_df.columns:
                participants_df["On General Waitlist"] = "No"  # Default to "No"
                st.info("Added 'On General Waitlist' column to participants.csv (defaulted to 'No').")
                made_changes_participants = True
            
            if made_changes_participants:
                participants_df.to_csv(participants_path, index=False)
        else:
            st.warning("participants.csv not found during migration 1.2.0->1.2.1. It should have been created by previous migration. If this is a fresh install, it's okay, schema will be applied on first load.")
        
        st.success("Successfully migrated to v1.2.1 for 'On General Waitlist' feature.")
        load_table.clear()
    except Exception as e:
        st.error(f"Migration to v1.2.1 (On General Waitlist) failed: {str(e)}")
        raise

# Migration for version 1.2.2: Adds 'Tags' to participants
def migrate_from_1_2_1_to_1_2_2():
    """Migration from v1.2.1 to v1.2.2:
    - Adds 'Tags' (comma-separated string) to participants.csv.
    """
    st.info("Starting migration to v1.2.2 for 'Tags' column...")
    try:
        participants_path = _path_for("participants")
        if os.path.exists(participants_path):
            participants_df = pd.read_csv(participants_path, dtype=str, na_filter=False).fillna("")
            if "Tags" not in participants_df.columns:
                participants_df["Tags"] = ""  # Default to empty string
                participants_df.to_csv(participants_path, index=False)
                st.info("Added 'Tags' column to participants.csv.")
        else:
            st.warning("participants.csv not found during migration 1.2.1->1.2.2. It should have been created by previous migrations. If this is a fresh install, it will be created with the new schema on first load.")
        
        st.success("Successfully migrated to v1.2.2, 'Tags' column added.")
        load_table.clear()
    except Exception as e:
        st.error(f"Migration to v1.2.2 ('Tags' column) failed: {str(e)}")
        raise

###############################################################################
# Configuration
###############################################################################
DATA_DIR = "data"              # Folder where CSV files live (created if needed)

# Default column mappings for employees table
DEFAULT_MAPPINGS = {
    "employees": {
        "Standard ID": "Standard ID", # Assumed to exist in CSV
        "Email": "Work Email Address", # This is what we expect in CSV for our internal "Email"
    }
}

# Each logical table maps to a CSV file and a list of its canonical columns
FILES = {
    "employees": ("employees.csv", ["Standard ID", "Email"]), # Core internal columns
    "workshops": ("workshops.csv", [
        "Workshop #", "Series", "Skill", "Goal",
        "Instances",       # Comma-separated Event IDs
        "Registered",      # Comma-separated Standard IDs
        "Participated"     # Comma-separated Standard IDs
    ]),
    "cohorts": ("cohorts.csv", [
        "Name", "Date Started",
        "Nominated",       # Comma-separated Standard IDs
        "Invited",         # Comma-separated Standard IDs
        "Joined"           # Comma-separated Standard IDs
    ]),
    "events": ("events.csv", [
        "Event ID", "Name", "Date",
        "Category",         # Demo, Workshop, Meeting, Conference
        "Workshop",         # If Category == Workshop ➜ Workshop #
        "Hosted",        # Comma-separated Standard IDs of hosts (now managed via Participants)
        "Registrations",    # Comma-separated Standard IDs
        "Participants"     # Comma-separated Standard IDs
    ]),
    "participants": ("participants.csv", [
        "Standard ID", "Email",
        "Events Registered",    # Comma-separated Event IDs
        "Events Participated",  # Comma-separated Event IDs
        "Events Hosted",        # Comma-separated Event IDs
        "Waitlist",             # "Yes" or "No"
        "Cohorts Nominated",    # Comma-separated Cohort Names
        "Cohorts Invited",      # Comma-separated Cohort Names
        "Cohorts Joined",       # Comma-separated Cohort Names
        "Nominated By",         # Comma-separated Employee Emails/IDs
        "Tags",                 # Comma-separated string of tags
        "Last Updated"          # Timestamp
    ])
}

# Event categories and their descriptions
EVENT_CATEGORIES = {
    "Workshop": "Official workshop instance from the workshop series",
    "Demo": "Product demonstration or showcase",
    "Meeting": "Team or group meeting",
    "Conference": "Large-scale event or conference"
}

###############################################################################
# Utility helpers
###############################################################################
def ensure_data_dir() -> None:
    """Create data directory if it doesn't exist."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)

def _path_for(key: str) -> str:
    """Absolute CSV path for a given logical table key."""
    filename, _ = FILES[key]
    return os.path.join(DATA_DIR, filename)

def validate_and_fix_csv_schema(key: str, df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """Validate CSV against expected schema and fix if necessary."""
    canonical_cols = FILES[key][1][:]  # Make a copy of expected columns
    fixed = False
    
    # Check if all expected columns exist
    for col in canonical_cols:
        if col not in df.columns:
            df[col] = ""
            fixed = True
            st.warning(f"Added missing column '{col}' to {key} table")
    
    return df, fixed


@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_table(key: str) -> pd.DataFrame:
    """Load the CSV for *key* – if missing, create an empty one first."""
    path = _path_for(key)
    # canonical_cols are the *minimum* internal columns we expect (e.g., "Standard ID", "Email")
    # plus any other columns found in the CSV for employees.
    canonical_cols = FILES[key][1][:] # Make a copy

    if os.path.exists(path):
        df = pd.read_csv(
            path,
            dtype=str, # Initially read all as string
            na_filter=False,
            low_memory=True,
            engine='c'
        ).fillna("")

        if key == "employees":
            # Force cache clear for employees table
            load_table.clear()
            # Rename "Work Email Address" to "Email" if it exists
            if "Work Email Address" in df.columns:
                df = df.rename(columns={"Work Email Address": "Email"})
            
            # Ensure "Standard ID" and "Email" are present
            if "Standard ID" not in df.columns:
                df["Standard ID"] = ""
            if "Email" not in df.columns:
                df["Email"] = "" # This would happen if "Work Email Address" wasn't in CSV
            
            # Dynamically add any other columns from the CSV to our list of columns to display/use
            # The initial canonical_cols for employees is ["Standard ID", "Email"]
            for col in df.columns:
                if col not in canonical_cols:
                    canonical_cols.append(col)

        # Special handling for date columns (for other tables)
        elif key == "events" and "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
        elif key == "cohorts" and "Date Started" in df.columns:
            df["Date Started"] = pd.to_datetime(df["Date Started"], errors='coerce')

        # Validate and fix schema if necessary
        df, was_fixed = validate_and_fix_csv_schema(key, df)
        if was_fixed:
            # If we had to fix the schema, save the fixed file
            df_to_save = df.copy()
            if key == "employees":
                # If we were to save with external names, this is where we'd rename "Email" back
                df_to_save = df_to_save.rename(columns={"Email": "Work Email Address"})
            df_to_save.to_csv(path, index=False)

    else: # File does not exist, create an empty one with canonical columns
        df = pd.DataFrame(columns=canonical_cols) 
        # If creating a new table, ensure date columns are datetime type
        if key == "events" and "Date" in canonical_cols:
            df["Date"] = pd.Series(dtype='datetime64[ns]')
        elif key == "cohorts" and "Date Started" in canonical_cols:
            df["Date Started"] = pd.Series(dtype='datetime64[ns]')
        
        # For a new employees.csv, we need to map internal "Email" back to "Work Email Address"
        # if we were to save it immediately
        df_to_save = df.copy()
        if key == "employees":
             # If we were to save with external names, this is where we'd rename "Email" back
             df_to_save = df_to_save.rename(columns={"Email": "Work Email Address"})
        df_to_save.to_csv(path, index=False)

    # Ensure all *expected* (canonical + dynamic for employees) columns exist in the DataFrame
    # For employees, canonical_cols has already been updated with dynamic columns from CSV
    for col in canonical_cols:
        if col not in df.columns:
            df[col] = ""
    
    # Re-ensure date columns are datetime if they were re-added as string
    if key == "events" and "Date" in df.columns and df["Date"].dtype == object:
        df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    elif key == "cohorts" and "Date Started" in df.columns and df["Date Started"].dtype == object:
        df["Date Started"] = pd.to_datetime(df["Date Started"], errors='coerce')

    return df[canonical_cols]   # Enforce column order, including dynamic ones for employees


def save_table(key: str, df: pd.DataFrame) -> None:
    """Persist *df* back to disk for logical table *key*."""
    path = _path_for(key)
    
    # For employees, convert internal "Email" back to "Work Email Address" when saving
    df_to_save = df.copy()
    if key == "employees" and "Email" in df.columns:
        df_to_save = df_to_save.rename(columns={"Email": "Work Email Address"})
        
    df_to_save.to_csv(path, index=False)


def get_employee_ids_from_input(input_str: str, all_employees: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Parses a string of IDs/emails, validates them, and returns valid IDs and invalids."""
    identifiers = [item.strip() for item in input_str.strip().split('\n') if item.strip()]
    valid_ids = set()
    invalid_inputs = []

    email_to_id = all_employees.set_index("Email")["Standard ID"]
    id_set = set(all_employees["Standard ID"])

    for identifier in identifiers:
        if '@' in identifier: # Assume it's an email
            if identifier in email_to_id.index:
                valid_ids.add(email_to_id[identifier])
            else:
                invalid_inputs.append(identifier)
        else: # Assume it's a Standard ID
            if identifier in id_set:
                valid_ids.add(identifier)
            else:
                invalid_inputs.append(identifier)
    return sorted(list(valid_ids)), invalid_inputs


def update_employee_event_status(employee_ids: list[str], event_id: str, mark_registered: Union[bool, None], mark_participated: Union[bool, None], mark_hosted: Union[bool, None]) -> tuple[int, int, int]:
    """Updates event status (Registered, Participated, Hosted) for employees by ADDING them to the respective lists if marked.
    Does NOT remove employees from lists if a mark is False/None. Removals must be handled manually if needed.
    """
    if not event_id or not employee_ids:
        return 0, 0, 0

    participants_df = load_table("participants")
    events_df = load_table("events")
    employees_df = load_table("employees")
    load_table.clear()

    event_row_series = events_df[events_df["Event ID"] == event_id]
    if event_row_series.empty:
        st.error(f"Event ID {event_id} not found in events.csv.")
        return 0, 0, 0
    event_idx = event_row_series.index[0]

    newly_registered_count = 0
    newly_participated_count = 0
    newly_hosted_count = 0
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- Update events.csv --- 
    event_registrations = set(str(events_df.loc[event_idx, "Registrations"]).split(',') if events_df.loc[event_idx, "Registrations"] else [])
    event_participants = set(str(events_df.loc[event_idx, "Participants"]).split(',') if events_df.loc[event_idx, "Participants"] else [])
    event_hosts = set(str(events_df.loc[event_idx, "Hosted"]).split(',') if events_df.loc[event_idx, "Hosted"] else [])

    # Store initial lengths to calculate *newly added* later
    initial_event_reg_len = len(event_registrations)
    initial_event_part_len = len(event_participants)
    initial_event_host_len = len(event_hosts)

    for emp_id in employee_ids:
        if mark_registered is True:
            event_registrations.add(emp_id)
        
        if mark_participated is True:
            event_participants.add(emp_id)

        if mark_hosted is True:
            event_hosts.add(emp_id)

    events_df.loc[event_idx, "Registrations"] = ",".join(sorted(list(filter(None, event_registrations))))
    events_df.loc[event_idx, "Participants"] = ",".join(sorted(list(filter(None, event_participants))))
    events_df.loc[event_idx, "Hosted"] = ",".join(sorted(list(filter(None, event_hosts))))

    if mark_registered is True: newly_registered_count = len(event_registrations) - initial_event_reg_len
    if mark_participated is True: newly_participated_count = len(event_participants) - initial_event_part_len
    if mark_hosted is True: newly_hosted_count = len(event_hosts) - initial_event_host_len

    # --- Update participants.csv --- 
    for emp_id in employee_ids:
        participant_indices = participants_df[participants_df["Standard ID"] == emp_id].index
        participant_idx = -1 
        if participant_indices.empty:
            emp_details = employees_df[employees_df["Standard ID"] == emp_id]
            email = emp_details["Email"].iloc[0] if not emp_details.empty else ""
            new_row_data = {col: "" for col in participants_df.columns}
            new_row_data["Standard ID"] = emp_id
            new_row_data["Email"] = email
            if "Waitlist" in new_row_data: new_row_data["Waitlist"] = "No"
            
            participants_df = pd.concat([participants_df, pd.DataFrame([new_row_data])], ignore_index=True)
            participant_idx = participants_df[participants_df["Standard ID"] == emp_id].index[0]
            st.info(f"Created new entry in participants.csv for {emp_id}")
        else:
            participant_idx = participant_indices[0]

        emp_events_registered = set(str(participants_df.loc[participant_idx, "Events Registered"]).split(',') if participants_df.loc[participant_idx, "Events Registered"] else [])
        emp_events_participated = set(str(participants_df.loc[participant_idx, "Events Participated"]).split(',') if participants_df.loc[participant_idx, "Events Participated"] else [])
        emp_events_hosted = set(str(participants_df.loc[participant_idx, "Events Hosted"]).split(',') if participants_df.loc[participant_idx, "Events Hosted"] else [])

        action_taken_on_participant_record = False
        if mark_registered is True and event_id not in emp_events_registered: 
            emp_events_registered.add(event_id); action_taken_on_participant_record = True
            
        if mark_participated is True and event_id not in emp_events_participated: 
            emp_events_participated.add(event_id); action_taken_on_participant_record = True

        if mark_hosted is True and event_id not in emp_events_hosted: 
            emp_events_hosted.add(event_id); action_taken_on_participant_record = True

        participants_df.loc[participant_idx, "Events Registered"] = ",".join(sorted(list(filter(None, emp_events_registered))))
        participants_df.loc[participant_idx, "Events Participated"] = ",".join(sorted(list(filter(None, emp_events_participated))))
        participants_df.loc[participant_idx, "Events Hosted"] = ",".join(sorted(list(filter(None, emp_events_hosted))))
        
        if action_taken_on_participant_record:
            participants_df.loc[participant_idx, "Last Updated"] = current_time

    save_table("events", events_df)
    save_table("participants", participants_df)
    load_table.clear()
    return newly_registered_count, newly_participated_count, newly_hosted_count


def update_cohort_membership(cohort_name: str, employee_ids: list[str], mark_nominated: bool, mark_invited: bool, mark_joined: bool) -> tuple[int, int, int]:
    """Adds employee IDs to the Nominated, Invited, and/or Joined fields for a given cohort."""
    if not cohort_name or not employee_ids or (not mark_nominated and not mark_invited and not mark_joined):
        return 0, 0, 0 # Nothing to do

    cohorts_df = load_table("cohorts")
    participants_df = load_table("participants") # Load participants.csv
    # load_table.clear() # Clear after all reads, before writes, or at the very end.

    cohort_index_list = cohorts_df.index[cohorts_df["Name"] == cohort_name].tolist()
    if not cohort_index_list:
        st.error(f"Cohort Name '{cohort_name}' not found.")
        return 0, 0, 0
    cohort_idx = cohort_index_list[0]

    added_nominees_count = 0
    added_invited_count = 0
    added_joined_count = 0
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # --- Update cohorts.csv --- 
    # For Nominated
    if mark_nominated:
        current_cohort_nominees = set(str(cohorts_df.loc[cohort_idx, "Nominated"]).split(',') if cohorts_df.loc[cohort_idx, "Nominated"] else [])
        initial_len = len(current_cohort_nominees)
        current_cohort_nominees.update(employee_ids)
        cohorts_df.loc[cohort_idx, "Nominated"] = ",".join(sorted(list(filter(None, current_cohort_nominees))))
        added_nominees_count = len(current_cohort_nominees) - initial_len

    # For Invited
    if mark_invited:
        current_cohort_invited = set(str(cohorts_df.loc[cohort_idx, "Invited"]).split(',') if cohorts_df.loc[cohort_idx, "Invited"] else [])
        initial_len_inv = len(current_cohort_invited)
        current_cohort_invited.update(employee_ids)
        cohorts_df.loc[cohort_idx, "Invited"] = ",".join(sorted(list(filter(None, current_cohort_invited))))
        added_invited_count = len(current_cohort_invited) - initial_len_inv

    # For Joined
    if mark_joined:
        current_cohort_joined = set(str(cohorts_df.loc[cohort_idx, "Joined"]).split(',') if cohorts_df.loc[cohort_idx, "Joined"] else [])
        initial_len_join = len(current_cohort_joined)
        current_cohort_joined.update(employee_ids)
        cohorts_df.loc[cohort_idx, "Joined"] = ",".join(sorted(list(filter(None, current_cohort_joined))))
        added_joined_count = len(current_cohort_joined) - initial_len_join

    # --- Update participants.csv ---
    participants_file_updated = False
    for emp_id in employee_ids:
        participant_indices = participants_df[participants_df["Standard ID"] == emp_id].index
        if not participant_indices.empty:
            participant_idx = participant_indices[0]
            participant_row_changed = False

            if mark_nominated:
                emp_cohorts_nominated = set(str(participants_df.loc[participant_idx, "Cohorts Nominated"]).split(',') if participants_df.loc[participant_idx, "Cohorts Nominated"] else [])
                if cohort_name not in emp_cohorts_nominated:
                    emp_cohorts_nominated.add(cohort_name)
                    participants_df.loc[participant_idx, "Cohorts Nominated"] = ",".join(sorted(list(filter(None, emp_cohorts_nominated))))
                    participant_row_changed = True
            
            if mark_invited:
                emp_cohorts_invited = set(str(participants_df.loc[participant_idx, "Cohorts Invited"]).split(',') if participants_df.loc[participant_idx, "Cohorts Invited"] else [])
                if cohort_name not in emp_cohorts_invited:
                    emp_cohorts_invited.add(cohort_name)
                    participants_df.loc[participant_idx, "Cohorts Invited"] = ",".join(sorted(list(filter(None, emp_cohorts_invited))))
                    participant_row_changed = True

            if mark_joined:
                emp_cohorts_joined = set(str(participants_df.loc[participant_idx, "Cohorts Joined"]).split(',') if participants_df.loc[participant_idx, "Cohorts Joined"] else [])
                if cohort_name not in emp_cohorts_joined:
                    emp_cohorts_joined.add(cohort_name)
                    participants_df.loc[participant_idx, "Cohorts Joined"] = ",".join(sorted(list(filter(None, emp_cohorts_joined))))
                    participant_row_changed = True
            
            if participant_row_changed:
                participants_df.loc[participant_idx, "Last Updated"] = current_time
                participants_file_updated = True
        else:
            st.warning(f"Employee ID {emp_id} not found in participants.csv. Cannot update their cohort status in participants.csv. Employee might be new or migration might be needed if this is unexpected.")

    save_table("cohorts", cohorts_df)
    if participants_file_updated:
        save_table("participants", participants_df)
    
    load_table.clear() 
    return added_nominees_count, added_invited_count, added_joined_count


###############################################################################
# Streamlit UI
###############################################################################
st.set_page_config(page_title="AI Adoption Program Tracker", layout="wide")
st.title("AI Adoption Program Tracker")

ensure_data_dir()

# Check schema version and run migrations if needed
current_schema_version = get_current_schema_version()
if current_schema_version != APP_VERSION:
    st.info(f"Checking for schema updates (current: v{current_schema_version}, latest: v{APP_VERSION})...")
    migration_result = run_migrations(current_schema_version, APP_VERSION)
    if not migration_result and current_schema_version != '0.0.0':
        st.warning(f"No migration path found from v{current_schema_version} to v{APP_VERSION}. " +
                  "The app will try to continue, but you may encounter issues.")

SECTION_NAMES = {
    "Participants": "manage_participation",
    "Events": "events",
    "Cohorts": "cohorts",
    "Workshop Series": "workshops",
    "Employees": "employees"
}
# Use index=0 to default to Participants view
default_index = 0
section_label = st.sidebar.selectbox("Section", list(SECTION_NAMES.keys()), index=default_index)
table_key = SECTION_NAMES[section_label]

# ---------------------------------------------------------------------------
# Load data & editor / section display
# ---------------------------------------------------------------------------

# --- Manage Participation Section ---
if table_key == "manage_participation":
    st.header("Participants")

    participants_df = load_table("participants")
    employees_df = load_table("employees")
    events_df = load_table("events")

    if participants_df.empty and not employees_df.empty:
        st.warning("Participants data is empty. If you have existing employee data, a migration might be pending or may need to be re-triggered. New participants will be created from the Employees table on first interaction if needed by certain operations.")
    elif employees_df.empty:
        st.warning("No employees found. Please add employees in the 'Employees' section first. The Participants view is based on the Employees list.")
    
    # Main display for participants table and its save logic
    if not participants_df.empty:
        column_config_participants_new = {
                "Standard ID": st.column_config.TextColumn("Standard ID", disabled=True),
                "Email": st.column_config.TextColumn("Email", disabled=True),
            "Events Registered": st.column_config.TextColumn("Events Registered (IDs)", help="Managed via \'Update Employee Event Status\' sidebar.", disabled=True),
            "Events Participated": st.column_config.TextColumn("Events Participated (IDs)", help="Managed via \'Update Employee Event Status\' sidebar.", disabled=True),
            "Events Hosted": st.column_config.TextColumn("Events Hosted (IDs)", help="Managed via \'Update Employee Event Status\' sidebar.", disabled=True),
            # "Events Waitlisted": st.column_config.TextColumn("Events Waitlisted (IDs)", help="Managed via \'Update Employee Event Status\' sidebar.", disabled=True), # Removed
            "Waitlist": st.column_config.CheckboxColumn("Waitlist", help="Is this participant on a general program waitlist?", default=False),
            "Cohorts Nominated": st.column_config.TextColumn("Cohorts Nominated (Names)", help="Managed via \'Cohorts\' section.", disabled=True),
            "Cohorts Invited": st.column_config.TextColumn("Cohorts Invited (Names)", help="Managed via \'Cohorts\' section.", disabled=True),
            "Cohorts Joined": st.column_config.TextColumn("Cohorts Joined (Names)", help="Managed via \'Cohorts\' section.", disabled=True),
            "Nominated By": st.column_config.TextColumn("Nominated By (Email/ID)", help="Manually enter who nominated this participant for any program/cohort."),
            "Tags": st.column_config.TextColumn("Tags", help="Comma-separated tags (e.g., Working Group Lead, Offering Support)"),
                "Last Updated": st.column_config.TextColumn("Last Updated", disabled=True)
            }

        for col_key in FILES["participants"][1]:
            if col_key not in participants_df.columns:
                participants_df[col_key] = ""
        
        participants_df_for_editor = participants_df[FILES["participants"][1]].copy()

        edited_participants_df = st.data_editor(
                participants_df_for_editor,
                num_rows="dynamic",
            key="editor_participants_new",
                use_container_width=True,
            column_config=column_config_participants_new,
            )

        if st.button("💾 Save Participant Details", key="save_participants_details_new"):
            current_participants_on_disk = load_table("participants")
            existing_ids_on_disk = set(current_participants_on_disk["Standard ID"])
            changes_detected = False
            processed_ids_from_editor = set()

            for idx_edited, edited_row in edited_participants_df.iterrows():
                std_id = edited_row["Standard ID"]
                processed_ids_from_editor.add(std_id)

                if not std_id:
                    st.warning(f"Skipping row {idx_edited + 1} in editor: Standard ID is missing. New participants should ideally be sourced from the Employees table.")
                    continue
                
                original_row_series_matches = current_participants_on_disk[current_participants_on_disk["Standard ID"] == std_id]
                row_changed_in_editor = False

                if not original_row_series_matches.empty:
                    original_row_idx = original_row_series_matches.index[0]
                    # Compare editable fields
                    if current_participants_on_disk.loc[original_row_idx, "Nominated By"] != edited_row["Nominated By"]:
                        current_participants_on_disk.loc[original_row_idx, "Nominated By"] = edited_row["Nominated By"]
                        row_changed_in_editor = True
                    
                    # Handle 'Waitlist' checkbox state
                    current_waitlist_status_str = str(current_participants_on_disk.loc[original_row_idx, "Waitlist"]).lower() == "yes"
                    editor_waitlist_status_bool = bool(edited_row["Waitlist"])
                    if current_waitlist_status_str != editor_waitlist_status_bool:
                        current_participants_on_disk.loc[original_row_idx, "Waitlist"] = "Yes" if editor_waitlist_status_bool else "No"
                        row_changed_in_editor = True
                    
                    # Handle 'Tags'
                    if current_participants_on_disk.loc[original_row_idx, "Tags"] != edited_row["Tags"]:
                        current_participants_on_disk.loc[original_row_idx, "Tags"] = edited_row["Tags"]
                        row_changed_in_editor = True

                    if row_changed_in_editor:
                        current_participants_on_disk.loc[original_row_idx, "Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        changes_detected = True
                else:
                    # New row added in the editor
                    if not edited_row["Email"] and std_id:
                        emp_match = employees_df[employees_df["Standard ID"] == std_id]
                        if not emp_match.empty:
                            edited_row["Email"] = emp_match["Email"].iloc[0]
                        else:
                            st.warning(f"New participant ID {std_id} added in editor, but not found in Employees table to fetch Email.")
                    
                    new_row_data = {col: "" for col in FILES["participants"][1]}
                    for col_name in FILES["participants"][1]:
                        if col_name in edited_row:
                            new_row_data[col_name] = edited_row[col_name]
                    # Ensure 'Waitlist' from editor is correctly converted for new row
                    new_row_data["Waitlist"] = "Yes" if bool(edited_row.get("Waitlist", False)) else "No"
                    new_row_data["Tags"] = edited_row.get("Tags", "") # Add Tags for new row
                    new_row_data["Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    current_participants_on_disk = pd.concat([current_participants_on_disk, pd.DataFrame([new_row_data])], ignore_index=True)
                    changes_detected = True
                    st.info(f"Added new participant: {std_id} via editor.")

            deleted_ids = existing_ids_on_disk - processed_ids_from_editor
            if deleted_ids:
                current_participants_on_disk = current_participants_on_disk[~current_participants_on_disk["Standard ID"].isin(deleted_ids)]
                changes_detected = True
                st.info(f"Removed {len(deleted_ids)} participant(s) via editor: {', '.join(deleted_ids)}")

            if changes_detected:
                save_table("participants", current_participants_on_disk[FILES["participants"][1]])
                st.success("Participant details saved successfully!")
                load_table.clear()
                st.rerun()
            else:
                st.info("No changes detected in participant details.")
    
    else: # This else corresponds to `if not participants_df.empty:`
        st.info("No participant records found. Populate the Employees table first. Participants are derived from Employees.")

    # Sidebar for updating employee event status - This is OUTSIDE the `if not participants_df.empty` block, 
    # but still within `if table_key == "manage_participation"`
    with st.sidebar.expander("📝 Update Employee Event Status", expanded=True):
        st.markdown("### Update Event Status for Employee(s)")
        
        if events_df.empty:
            st.warning("No events found. Please add events in the 'Events' section first.")
        elif employees_df.empty:
            st.warning("No employees found. Please add employees in the 'Employees' section first.")
        else:
            event_options = {f"{row['Event ID']} - {row['Name']} ({pd.to_datetime(row['Date']).strftime('%Y-%m-%d') if pd.notna(row['Date']) else 'No Date'})": row['Event ID']
                             for _, row in events_df.sort_values("Date", ascending=False).iterrows()}
            
            selected_event_id = None # Initialize selected_event_id
            if not event_options:
                 st.info("No events available to select.")
            else:
                selected_event_display = st.selectbox(
                "Select Event",
                options=list(event_options.keys())
            )
            selected_event_id = event_options.get(selected_event_display)

            st.divider()
            st.markdown("#### Select Employees")
            employee_ids_to_process = []

            tab_paste, tab_select, tab_upload = st.tabs(["Paste List", "Select from List", "Upload File"])

            with tab_paste:
                pasted_list = st.text_area("Paste Standard IDs or Emails (one per line)", key="participants_paste_new")
                if pasted_list:
                    valid_ids, invalid_inputs = get_employee_ids_from_input(pasted_list, employees_df)
                    if valid_ids: 
                        st.write(f"Found {len(valid_ids)} valid employee(s).")
                        employee_ids_to_process.extend(valid_ids) # Use extend for multiple sources
                    if invalid_inputs: 
                        st.warning(f"Could not find/validate: {', '.join(invalid_inputs)}")

            with tab_select:
                employee_display_options = [f"{row['Standard ID']} - {row['Email']}"
                                            for _, row in employees_df.iterrows()]
                selected_employees_multi = st.multiselect(
                    "Select Employees",
                    options=employee_display_options,
                    key="participants_multiselect_new"
                )
                if selected_employees_multi:
                    ids_from_select = [opt.split(' - ')[0] for opt in selected_employees_multi]
                    # Add only if not already added from paste tab (to avoid duplicates if user uses multiple tabs)
                    for item_id in ids_from_select:
                        if item_id not in employee_ids_to_process:
                            employee_ids_to_process.append(item_id)

            with tab_upload:
                uploaded_file = st.file_uploader("Upload .txt or .csv (one ID/email per line)", type=["txt", "csv"], key="participants_upload_new")
                if uploaded_file is not None:
                    stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                    file_content = stringio.read()
                    valid_ids_file, invalid_inputs_file = get_employee_ids_from_input(file_content, employees_df)
                    if valid_ids_file: 
                        st.write(f"Found {len(valid_ids_file)} valid employee(s) from file.")
                        for item_id in valid_ids_file:
                             if item_id not in employee_ids_to_process:
                                employee_ids_to_process.append(item_id)
                    if invalid_inputs_file: 
                        st.warning(f"Could not find/validate from file: {', '.join(invalid_inputs_file)}")
            
            # Remove duplicates just in case, preserving order as much as possible
            if employee_ids_to_process:
                employee_ids_to_process = sorted(list(set(employee_ids_to_process)), key=employee_ids_to_process.index)

            st.divider()
            st.markdown("#### Set Participation Status for Selected Event")
            
            # Stack checkboxes vertically instead of in columns
            set_registered = st.checkbox("Registered", key="set_registered_event_new_key")
            set_participated = st.checkbox("Participated", key="set_participated_event_new_key")
            set_hosted = st.checkbox("Hosted", key="set_hosted_event_new_key")
            # Removed the caption
            st.divider()

            update_button_disabled = not (selected_event_id and employee_ids_to_process and (set_registered or set_participated or set_hosted)) # Button enabled if any action is true
            
            if st.button("Update Event Status", disabled=update_button_disabled, key="update_event_status_button_new_key"):
                st.write(f"Processing updates for {len(employee_ids_to_process)} employee(s) for event {selected_event_id}: Adding Registered={set_registered}, Adding Participated={set_participated}, Adding Hosted={set_hosted}")

                newly_added_reg, newly_added_part, newly_added_host = update_employee_event_status(
                    employee_ids_to_process,
                    selected_event_id,
                    mark_registered=set_registered,
                    mark_participated=set_participated,
                    mark_hosted=set_hosted
                )
                
                msg_parts = []
                if set_registered and newly_added_reg > 0: msg_parts.append(f"{newly_added_reg} newly registered.")
                elif set_registered: msg_parts.append(f"Registrations: No new additions (already registered or list unchanged).")
                
                if set_participated and newly_added_part > 0: msg_parts.append(f"{newly_added_part} newly participated.")
                elif set_participated: msg_parts.append(f"Participation: No new additions (already participated or list unchanged).")

                if set_hosted and newly_added_host > 0: msg_parts.append(f"{newly_added_host} newly marked as hosted.")
                elif set_hosted: msg_parts.append(f"Hosted: No new additions (already marked as host or list unchanged).")
                
                if not msg_parts and not (set_registered or set_participated or set_hosted):
                     # This case should ideally be prevented by the disabled button logic
                     msg_parts.append("No actions selected.")
                elif not msg_parts: # Some action was selected, but no one was newly added
                    msg_parts.append("No new individuals were added to the selected lists (they may have already been on them).")

                event_display_str = selected_event_display if selected_event_id else "N/A" # Construct safely
                final_message = f"Event status update processed for {len(employee_ids_to_process)} employee(s) for event '{event_display_str}'."
                if msg_parts:
                    final_message += " Details: " + ' '.join(msg_parts)
                else: # Should ideally not happen with current logic if button was enabled
                    final_message += " No specific actions were performed or needed." 
                
                st.success(final_message)
                st.rerun()

    # New expander for bulk updating participant waitlist status and tags
    with st.sidebar.expander("📋 Update Participant Details", expanded=False):
        st.markdown("### Update Waitlist Status & Tags")
        
        if employees_df.empty:
            st.warning("No employees found. Please add employees in the 'Employees' section first.")
        else:
            st.markdown("#### Select Employees")
            bulk_employee_ids = []

            bulk_tab_paste, bulk_tab_select, bulk_tab_upload = st.tabs(["Paste List", "Select from List", "Upload File"])

            with bulk_tab_paste:
                bulk_pasted_list = st.text_area("Paste Standard IDs or Emails (one per line)", key="bulk_participants_paste")
                if bulk_pasted_list:
                    valid_ids, invalid_inputs = get_employee_ids_from_input(bulk_pasted_list, employees_df)
                    if valid_ids:
                        st.write(f"Found {len(valid_ids)} valid employee(s).")
                        bulk_employee_ids.extend(valid_ids)
                    if invalid_inputs:
                        st.warning(f"Could not find/validate: {', '.join(invalid_inputs)}")

            with bulk_tab_select:
                employee_display_options = [f"{row['Standard ID']} - {row['Email']}"
                                           for _, row in employees_df.iterrows()]
                bulk_selected_employees = st.multiselect(
                    "Select Employees",
                    options=employee_display_options,
                    key="bulk_participants_multiselect"
                )
                if bulk_selected_employees:
                    ids_from_select = [opt.split(' - ')[0] for opt in bulk_selected_employees]
                    for item_id in ids_from_select:
                        if item_id not in bulk_employee_ids:
                            bulk_employee_ids.append(item_id)

            with bulk_tab_upload:
                bulk_uploaded_file = st.file_uploader("Upload .txt or .csv (one ID/email per line)", type=["txt", "csv"], key="bulk_participants_upload")
                if bulk_uploaded_file is not None:
                    stringio = io.StringIO(bulk_uploaded_file.getvalue().decode("utf-8"))
                    file_content = stringio.read()
                    valid_ids_file, invalid_inputs_file = get_employee_ids_from_input(file_content, employees_df)
                    if valid_ids_file:
                        st.write(f"Found {len(valid_ids_file)} valid employee(s) from file.")
                        for item_id in valid_ids_file:
                            if item_id not in bulk_employee_ids:
                                bulk_employee_ids.append(item_id)
                    if invalid_inputs_file:
                        st.warning(f"Could not find/validate from file: {', '.join(invalid_inputs_file)}")
            
            # Remove duplicates just in case, preserving order as much as possible
            if bulk_employee_ids:
                bulk_employee_ids = sorted(list(set(bulk_employee_ids)), key=bulk_employee_ids.index)
                st.write(f"**Total unique employees selected:** {len(bulk_employee_ids)}")

            st.divider()
            st.markdown("#### Set Details to Update")
            
            # Waitlist status options
            waitlist_action = st.radio(
                "Update Waitlist Status:",
                ["No Change", "Add to Waitlist", "Remove from Waitlist"],
                index=0,
                key="waitlist_action"
            )
            
            # Tags input
            tags_to_add = st.text_input(
                "Add Tags (comma-separated):",
                help="These tags will be added to existing tags for each employee",
                key="tags_to_add"
            )
            
            st.divider()
            
            # Update button
            update_details_disabled = not (bulk_employee_ids and (waitlist_action != "No Change" or tags_to_add.strip()))
            
            if st.button("Update Participant Details", disabled=update_details_disabled, key="update_participant_details_button"):
                participants_df = load_table("participants")
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                updates_made = 0
                waitlist_updates = 0
                tag_updates = 0
                
                # Process each employee
                for emp_id in bulk_employee_ids:
                    # Find or create participant entry
                    participant_indices = participants_df[participants_df["Standard ID"] == emp_id].index
                    participant_idx = -1
                    
                    if participant_indices.empty:
                        # Create new participant record
                        emp_details = employees_df[employees_df["Standard ID"] == emp_id]
                        email = emp_details["Email"].iloc[0] if not emp_details.empty else ""
                        new_row_data = {col: "" for col in participants_df.columns}
                        new_row_data["Standard ID"] = emp_id
                        new_row_data["Email"] = email
                        new_row_data["Waitlist"] = "No"
                        
                        participants_df = pd.concat([participants_df, pd.DataFrame([new_row_data])], ignore_index=True)
                        participant_idx = participants_df[participants_df["Standard ID"] == emp_id].index[0]
                        st.info(f"Created new entry in participants.csv for {emp_id}")
                    else:
                        participant_idx = participant_indices[0]
                    
                    row_updated = False
                    
                    # Update waitlist status if requested
                    if waitlist_action != "No Change":
                        new_waitlist_status = "Yes" if waitlist_action == "Add to Waitlist" else "No"
                        current_status = str(participants_df.loc[participant_idx, "Waitlist"]).strip().lower()
                        
                        # Only update if changing from current state
                        if (new_waitlist_status.lower() == "yes" and current_status != "yes") or \
                           (new_waitlist_status.lower() == "no" and current_status == "yes"):
                            participants_df.loc[participant_idx, "Waitlist"] = new_waitlist_status
                            row_updated = True
                            waitlist_updates += 1
                    
                    # Add tags if provided
                    if tags_to_add.strip():
                        # Get current tags, split, and ensure we have a clean list
                        current_tags = str(participants_df.loc[participant_idx, "Tags"])
                        current_tag_list = [tag.strip() for tag in current_tags.split(",") if tag.strip()]
                        
                        # Get new tags, split, and ensure we have a clean list
                        new_tag_list = [tag.strip() for tag in tags_to_add.split(",") if tag.strip()]
                        
                        # Add new tags if they don't already exist
                        added_tags = False
                        for new_tag in new_tag_list:
                            if new_tag not in current_tag_list:
                                current_tag_list.append(new_tag)
                                added_tags = True
                        
                        if added_tags:
                            # Update the Tags field with the combined list
                            participants_df.loc[participant_idx, "Tags"] = ", ".join(current_tag_list)
                            row_updated = True
                            tag_updates += 1
                    
                    # Update timestamp if any changes were made
                    if row_updated:
                        participants_df.loc[participant_idx, "Last Updated"] = current_time
                        updates_made += 1
                
                # Save changes if any were made
                if updates_made > 0:
                    save_table("participants", participants_df)
                    
                    success_msg = []
                    if waitlist_action != "No Change":
                        status_text = "added to" if waitlist_action == "Add to Waitlist" else "removed from"
                        success_msg.append(f"{waitlist_updates} participant(s) {status_text} waitlist")
                    
                    if tags_to_add.strip():
                        success_msg.append(f"{tag_updates} participant(s) had tags added")
                    
                    st.success(f"Successfully updated {updates_made} participant(s). {' and '.join(success_msg)}.")
                    load_table.clear()
                    st.rerun()
                else:
                    st.info("No updates were needed. Participants may already have the specified waitlist status or tags.")

# --- Other Sections (Employees, Workshops, Cohorts, Events) ---
else:
    if table_key == "events":
        load_table.clear() 
    df = load_table(table_key)
    st.header(section_label)

    # For employees table, allow toggling display of dynamic columns
    displayed_columns = FILES[table_key][1][:] # Start with core internal columns

    if table_key == "employees":
        # df already contains all columns from CSV, including dynamic ones.
        # FILES["employees"][1] is just ["Standard ID", "Email"]
        # We want to offer selection for columns in df that are NOT these two.
        all_available_columns = list(df.columns)
        optional_columns = [col for col in all_available_columns if col not in ["Standard ID", "Email"]]
        
        if optional_columns:
            with st.sidebar.expander("📊 Display Options", expanded=True):
                st.markdown("### Display Columns")
                selected_optional_cols = st.multiselect(
                    "Select additional columns to display:",
                    options=optional_columns,
                    default=[] # Initially, only show Standard ID and Email
                )
                displayed_columns.extend(selected_optional_cols)
        
        # Ensure Standard ID and Email are always first and present, even if user deselects them (though they can't)
        if "Email" not in displayed_columns:
            displayed_columns.insert(0, "Email") # Should be there via FILES[table_key][1]
        if "Standard ID" not in displayed_columns:
            displayed_columns.insert(0, "Standard ID") # Should be there via FILES[table_key][1]
        # Remove duplicates and maintain order (Standard ID, Email, then selected optional)
        displayed_columns = sorted(list(set(displayed_columns)), key=lambda x: (x != "Standard ID", x != "Email", x))


    # Pagination for large datasets
    # Use df[displayed_columns] for pagination and display
    df_for_display = df[displayed_columns]
    df_display_paginated = df_for_display # Default to full display
    
    if len(df_for_display) > 1000:
        with st.sidebar.expander("📄 Pagination Controls", expanded=True):
            page_size = st.slider("Rows per page", 100, 1000, 500, key=f"pagesize_{table_key}")
            total_pages = len(df_for_display) // page_size + (1 if len(df_for_display) % page_size > 0 else 0)
            page = st.number_input("Page", 1, total_pages, 1, key=f"page_{table_key}")
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            df_display_paginated = df_for_display.iloc[start_idx:end_idx]
            st.caption(f"Showing {start_idx + 1}-{min(end_idx, len(df_for_display))} of {len(df_for_display)} rows")
    else:
        df_display_paginated = df_for_display # show all if less than 1000

    # --- Specific Editor Configurations ---

    # Custom editor for Events section
    if table_key == "events":
        # Prepare column configurations for Events data_editor
        workshop_df_for_options = load_table("workshops")
        
        # MODIFIED: Use actual Workshop # IDs for the SelectboxColumn options
        valid_workshop_ids = [""] # Start with a blank option for "no workshop"
        if not workshop_df_for_options.empty:
            # Get unique Workshop # values and add them to the list
            valid_workshop_ids.extend(workshop_df_for_options["Workshop #"].unique().tolist())

        column_config_events = {
            "Workshop": st.column_config.SelectboxColumn(
                "Workshop", help="Select the workshop this event is an instance of.",
                options=valid_workshop_ids, # Use the actual IDs here
                required=False
            ),
            "Registrations": st.column_config.TextColumn("Registered", help="Employee IDs registered (updated via Manage Participation).", disabled=True),
            "Participants": st.column_config.TextColumn("Participated", help="Employee IDs participated (updated via Manage Participation).", disabled=True),
            "Hosted": st.column_config.TextColumn("Hosted", help="Employee IDs hosted (updated via Manage Participation).", disabled=True),
            # "Waitlisted": st.column_config.TextColumn("Waitlisted IDs", help="Employee IDs waitlisted (updated via Manage Participation).", disabled=True), # Removed
            "Event ID": st.column_config.TextColumn("Event ID", disabled=True),
            "Date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            "Category": st.column_config.SelectboxColumn("Category", options=list(EVENT_CATEGORIES.keys()), required=True),
        }

        edited_df = st.data_editor(
            df_display_paginated, num_rows="dynamic", key=f"editor_{table_key}",
            use_container_width=True, column_config=column_config_events
        )

        # Logic to handle combining form additions with table edits
        if 'newly_added_event' in st.session_state:
            new_event_df = st.session_state.pop('newly_added_event')
            target_df = edited_df if len(df) <= 1000 else df
            updated_df = pd.concat([target_df, new_event_df], ignore_index=True)
            save_table(table_key, updated_df)
            st.success("Event added and saved!")
            load_table.clear()
            st.rerun()

        # Sidebar: Add-new-event form wrapped in an expander for tidier layout
        with st.sidebar.expander("➕ Add New Event", expanded=False):
            with st.form("new_event_form"):
                event_name = st.text_input("Event Name")
                event_date = st.date_input("Event Date")
                event_category = st.selectbox("Category", options=list(EVENT_CATEGORIES.keys()), help="Select the type of event")
                
                workshop_df_form = load_table("workshops")
                form_workshop_options = [f"{row['Workshop #']} - {row['Skill']}: {row['Goal']}" for _, row in workshop_df_form.iterrows()]
                form_workshop_options.insert(0, "")
                selected_workshop_display = st.selectbox("Workshop (if applicable)", options=form_workshop_options, help="Select the workshop this event is an instance of (if applicable)") if event_category == "Workshop" else ""
                selected_workshop_id = selected_workshop_display.split(" - ")[0] if selected_workshop_display and " - " in selected_workshop_display else ""
                
                submitted = st.form_submit_button("Add Event")
                if submitted:
                    base_df_for_id = edited_df if len(df) <= 1000 else df
                    prefix_map = {"Workshop": "W", "Demo": "D", "Meeting": "M", "Conference": "C"}
                    prefix = prefix_map.get(event_category, "E")
                    date_str = event_date.strftime("%Y%m%d")
                    existing_ids = base_df_for_id[base_df_for_id["Event ID"].str.startswith(f"{prefix}{date_str}-")]
                    if existing_ids.empty: next_seq = 1
                    else: next_seq = existing_ids["Event ID"].str.split("-").str[-1].astype(int).max() + 1
                    event_id = f"{prefix}{date_str}-{next_seq:02d}"

                    new_event = pd.DataFrame([{
                        "Event ID": event_id, "Name": event_name, 
                        "Date": pd.to_datetime(event_date.strftime("%Y-%m-%d"), errors='coerce'),
                        "Category": event_category, "Workshop": selected_workshop_id,
                        "Hosted": "",  
                        "Registrations": "", "Participants": ""
                        # "Waitlisted": "" # Removed
                    }])
                    st.session_state['newly_added_event'] = new_event
                    st.rerun()

        if st.button("💾  Save", key=f"save_{table_key}"):
            if len(df) > 1000:
                df.iloc[start_idx:end_idx] = edited_df
                save_table(table_key, df)
            else:
                save_table(table_key, edited_df)
            st.success("Table changes saved to disk.")
            load_table.clear()
            st.rerun()

    # Custom section for Cohorts
    elif table_key == "cohorts":
        employees_df = load_table("employees") # Needed for employee selection

        column_config_cohorts = {
            "Nominated": st.column_config.TextColumn("Nominated", help="Comma-separated Standard IDs.", disabled=True),
            "Invited": st.column_config.TextColumn("Invited", help="Comma-separated Standard IDs.", disabled=True),
            "Joined": st.column_config.TextColumn("Joined", help="Comma-separated Standard IDs.", disabled=True),
            "Date Started": st.column_config.DateColumn("Date Started", format="YYYY-MM-DD", required=True)
        }
        edited_df = st.data_editor(
            df_display_paginated, num_rows="dynamic", key=f"editor_{table_key}",
            use_container_width=True, column_config=column_config_cohorts
        )

        # Logic to handle combining form additions with table edits
        if 'newly_added_cohort' in st.session_state:
            new_cohort_df = st.session_state.pop('newly_added_cohort')
            target_df = edited_df if len(df) <= 1000 else df # Use edited if not paginated
            updated_df = pd.concat([target_df, new_cohort_df], ignore_index=True)
            save_table(table_key, updated_df)
            st.success("Cohort added and saved!")
            load_table.clear()
            st.rerun()

        # Save Button for Table Edits
        if st.button("💾  Save", key=f"save_{table_key}"):
             if len(df) > 1000:
                df.iloc[start_idx:end_idx] = edited_df
                save_table(table_key, df)
             else:
                 save_table(table_key, edited_df) # Save directly if not paginated
             st.success("Table changes saved to disk.")
             load_table.clear()
             st.rerun()

        # Sidebar: Add-new-cohort form wrapped in an expander for tidier layout
        with st.sidebar.expander("➕ Add New Cohort", expanded=False):
            with st.form("new_cohort_form"):
                cohort_name = st.text_input("Cohort Name")
                cohort_date = st.date_input("Date Started")
                submitted = st.form_submit_button("Add Cohort")
                if submitted and cohort_name:
                    # Check if cohort name already exists (using potentially edited df)
                    check_df = edited_df if len(df) <= 1000 else df
                    if cohort_name in check_df["Name"].values:
                         st.error(f"Cohort with name '{cohort_name}' already exists.")
                    else:
                        new_cohort = pd.DataFrame([{
                            "Name": cohort_name,
                            "Date Started": pd.to_datetime(cohort_date.strftime("%Y-%m-%d"), errors='coerce'),
                            "Nominated": "",
                            "Invited": "",
                            "Joined": ""
                        }])
                        st.session_state['newly_added_cohort'] = new_cohort
                        st.rerun()
                elif submitted:
                    st.warning("Cohort Name cannot be empty.")

        st.sidebar.divider()

        # Manage Cohort Membership
        with st.sidebar.expander("👥 Manage Cohort Members", expanded=True):
            st.markdown("### Manage Cohort Membership")
            if df.empty:
                st.warning("No cohorts exist yet. Add a cohort first.")
            elif employees_df.empty:
                st.warning("No employees found. Please add employees in the 'Employees' section first.")
            else:
                # Cohort Selection
                cohort_options = {row['Name']: row['Name'] for _, row in df.iterrows()}
                selected_cohort_name = st.selectbox(
                    "Select Cohort",
                    options=list(cohort_options.keys())
                )

                st.divider()

                # Input Employee IDs/Emails
                st.markdown("#### Select Employees")
                input_method = st.tabs(["Paste List", "Select from List", "Upload File"])
                employee_ids_to_process = []
                invalid_inputs_detected = []

                with input_method[0]: # Paste List
                    pasted_list = st.text_area("Paste Standard IDs or Emails (one per line)", key="cohort_paste")
                    if pasted_list:
                        employee_ids_to_process, invalid_inputs_detected = get_employee_ids_from_input(pasted_list, employees_df)
                        st.write(f"Found {len(employee_ids_to_process)} valid employee(s).")
                        if invalid_inputs_detected:
                            st.warning(f"Could not find/validate: {', '.join(invalid_inputs_detected)}")

                with input_method[1]: # Select from List
                    employee_display_options = [f"{row['Standard ID']} - {row['Email']}"
                                                for _, row in employees_df.iterrows()]
                    selected_employees_from_multiselect_cohorts = st.multiselect(
                        "Select Employees",
                        options=employee_display_options,
                        key="cohort_multiselect_input"
                    )
                    if selected_employees_from_multiselect_cohorts:
                        # If employee_ids_to_process is empty (i.e., not filled by paste/upload), then use these.
                        if not employee_ids_to_process:
                            employee_ids_to_process = [opt.split(' - ')[0] for opt in selected_employees_from_multiselect_cohorts]

                with input_method[2]: # Upload File
                    uploaded_file = st.file_uploader("Upload .txt or .csv (one ID/email per line)", type=["txt", "csv"], key="cohort_upload")
                    if uploaded_file is not None:
                        stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                        file_content = stringio.read()
                        employee_ids_to_process, invalid_inputs_detected = get_employee_ids_from_input(file_content, employees_df)
                        st.write(f"Found {len(employee_ids_to_process)} valid employee(s).")
                        if invalid_inputs_detected:
                            st.warning(f"Could not find/validate: {', '.join(invalid_inputs_detected)}")

                st.divider()

                # Select Membership Type
                st.markdown("#### Set Membership Status")
                mark_nominated = st.checkbox("Nominated")
                mark_invited = st.checkbox("Invited")
                mark_joined = st.checkbox("Joined")

                st.divider()

                # Update Button
                if st.button("Update Cohort Membership", disabled=(not selected_cohort_name or not employee_ids_to_process or (not mark_nominated and not mark_invited and not mark_joined))):
                    added_nom, added_invited, added_joined = update_cohort_membership(
                        selected_cohort_name,
                        employee_ids_to_process,
                        mark_nominated,
                        mark_invited,
                        mark_joined
                    )
                    success_msgs = []
                    if mark_nominated:
                         success_msgs.append(f"Added {added_nom} new nomination(s).")
                    if mark_invited:
                         success_msgs.append(f"Added {added_invited} new invitation(s).")
                    if mark_joined:
                        success_msgs.append(f"Added {added_joined} new member(s) to cohort.")

                    if success_msgs:
                        st.success(f"Successfully updated cohort '{selected_cohort_name}'. {' '.join(success_msgs)}")
                        st.rerun()
                    else:
                        st.info("No changes made (employees might already have the selected status or cohort name not found).")

    # Standard editor for other sections
    else:
        _editor_key = f"editor_{table_key}"
        if table_key == "workshops":
            if "workshops_editor_version" not in st.session_state:
                st.session_state["workshops_editor_version"] = 0
            _editor_key = f"editor_workshops_{st.session_state['workshops_editor_version']}"

        edited_df_subset = st.data_editor(
            df_display_paginated, 
            num_rows="dynamic", 
            key=_editor_key,
            use_container_width=True
        )
        if st.button("💾  Save", key=f"save_{table_key}"):
            # df is the original full DataFrame loaded from load_table()
            # df_for_display is df[displayed_columns] (original data, subset of columns)
            # edited_df_subset is the result of st.data_editor on df_display_paginated (a page or full, with displayed_columns and dynamic rows)

            current_df_with_displayed_columns = None
            if len(df_for_display) > 1000: # PAGINATED CASE
                part_before = df_for_display.iloc[:start_idx].reset_index(drop=True)
                part_after = df_for_display.iloc[end_idx:].reset_index(drop=True)
                
                current_df_with_displayed_columns = pd.concat([
                    part_before, 
                    edited_df_subset.reset_index(drop=True), # This is the edited page
                    part_after
                ], ignore_index=True)
            else: # NON-PAGINATED CASE
                # edited_df_subset is the full table (but with only displayed_columns), edited.
                current_df_with_displayed_columns = edited_df_subset.reset_index(drop=True)

            # Now, current_df_with_displayed_columns has the correct rows and includes only the displayed_columns.
            # We need to build df_to_save, which will have ALL original columns.
            df_to_save = current_df_with_displayed_columns.copy()

            for original_col_name in df.columns: # Iterate over all column names from the original full df
                if original_col_name not in df_to_save.columns: # If this column is not in our current (displayed_columns only) df
                    # This means it's a non-displayed column. We need to add it.
                    # Get its values from the original 'df', aligned by the new index of df_to_save.
                    # Rows in df_to_save that are new (not in original df.index) will get NaN.
                    if original_col_name in df: # Check if column exists in original df
                        df_to_save[original_col_name] = df[original_col_name].reindex(df_to_save.index)
                    else: # Should not happen if df.columns is accurate
                        df_to_save[original_col_name] = pd.Series(index=df_to_save.index, dtype='object')


            # Fill any NaNs that resulted (e.g., for new rows in non-displayed columns, or if original data was NaN)
            df_to_save = df_to_save.fillna("")
            
            # Ensure all original columns are present and in the original order
            df_to_save = df_to_save.reindex(columns=df.columns, fill_value="")

            save_table(table_key, df_to_save)
            st.success("Saved to disk.")
            load_table.clear() # Clear cache after saving
            if table_key == "workshops":
                st.session_state["workshops_editor_version"] += 1
            st.rerun()


    # ---------------------------------------------------------------------------
    # Context-specific helpers (Only for non-participation sections)
    # ---------------------------------------------------------------------------
    # Removed Quick Category Tagger for Employees section

# Add version info and admin controls at the very bottom of the sidebar
st.sidebar.divider()
# Display version info in a small, subtle way
st.sidebar.caption(f"App Version: v{APP_VERSION}")

# Add backup controls in a collapsed "System" expander at the bottom
with st.sidebar.expander("⚙️ System", expanded=False):
    st.markdown("### Data Backup & Restore")
    if st.button("Create Backup Now", key="create_backup_btn"):
        backup_path = create_backup()
        if backup_path:
            st.success(f"Backup created at: {backup_path}")
        else:
            st.error("Failed to create backup")
    
    # List available backups
    if os.path.exists(BACKUP_DIR):
        backups = [d for d in os.listdir(BACKUP_DIR) if os.path.isdir(os.path.join(BACKUP_DIR, d))]
        if backups:
            backups.sort(reverse=True)  # Most recent first
            selected_backup = st.selectbox("Available Backups", options=backups, key="backup_select")
            if st.button("Restore Selected Backup", key="restore_backup_btn"):
                backup_path = os.path.join(BACKUP_DIR, selected_backup)
                # Confirm before restoring
                confirm = st.checkbox("I understand this will overwrite current data", key="confirm_restore")
                if confirm and st.button("Confirm Restore", key="confirm_restore_btn"):
                    # Create a backup of current data before restoring
                    create_backup()
                    # Copy files from backup to data directory
                    for file_name in os.listdir(backup_path):
                        if file_name.endswith('.csv'):
                            source_path = os.path.join(backup_path, file_name)
                            dest_path = os.path.join(DATA_DIR, file_name)
                            shutil.copy2(source_path, dest_path)
                    st.success("Backup restored successfully!")
                    # Clear cache to reload data
                    load_table.clear()
                    st.rerun()
        else:
            st.info("No backups available")
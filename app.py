import os
from datetime import datetime
import pandas as pd
import streamlit as st
import io # Needed for file uploads
import shutil # For file operations
import json # For version control

###############################################################################
# Version Control & Migration
###############################################################################
APP_VERSION = "1.0.0"  # Current app version
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
        '0.0.0->1.0.0': migrate_from_0_to_1
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
        "Nominated",        # Comma-separated Standard IDs
        "Participants"      # Comma-separated Standard IDs
    ]),
    "events": ("events.csv", [
        "Event ID", "Name", "Date",
        "Category",         # Demo, Workshop, Meeting, Conference
        "Workshop",         # If Category == Workshop ➜ Workshop #
        "Hosted",        # Comma-separated Standard IDs of hosts (now managed via Participants)
        "Registrations",    # Comma-separated Standard IDs
        "Participants"      # Comma-separated Standard IDs
    ]),
    "participants": ("participants.csv", [
        "Standard ID", "Email", "Event ID", "Event Name", "Event Date",
        "Registered",       # New: "Yes" or "No" (or blank for new)
        "Participated",     # New: "Yes" or "No" (or blank for new)
        "Hosted",           # New: "Yes" or "No" (participant also hosted this event)
        "Last Updated"      # Timestamp of last update
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


def update_event_participation(event_id: str, employee_ids: list[str], mark_registered: bool, mark_participated: bool, mark_hosted: bool) -> tuple[int, int, int]:
    """Adds employee IDs to the Registrations, Participants, and Hosted fields for a given event.
    Updates participant records, additively setting Registered/Participated/Hosted status if marked."""
    if not event_id or not employee_ids:
        return 0, 0, 0

    events_df = load_table("events")
    employees_df = load_table("employees")
    participants_df = load_table("participants")
    load_table.clear()

    event_index = events_df.index[events_df["Event ID"] == event_id].tolist()
    if not event_index:
        st.error(f"Event ID {event_id} not found.")
        return 0, 0, 0

    idx = event_index[0]
    event_name = events_df.loc[idx, "Name"]
    event_date = events_df.loc[idx, "Date"]
    added_to_event_registered_list = 0
    added_to_event_participated_list = 0
    added_to_event_hosted_list = 0
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Update events.csv lists
    # Registrations list
    current_event_registrations = set(events_df.loc[idx, "Registrations"].split(',') if events_df.loc[idx, "Registrations"] else [])
    initial_len_reg = len(current_event_registrations)
    if mark_registered:
        current_event_registrations.update(employee_ids)
    events_df.loc[idx, "Registrations"] = ",".join(sorted(list(filter(None, current_event_registrations))))
    if mark_registered:
      added_to_event_registered_list = len(current_event_registrations) - initial_len_reg

    # Participants list
    current_event_participants = set(events_df.loc[idx, "Participants"].split(',') if events_df.loc[idx, "Participants"] else [])
    initial_len_part = len(current_event_participants)
    if mark_participated:
        current_event_participants.update(employee_ids)
    events_df.loc[idx, "Participants"] = ",".join(sorted(list(filter(None, current_event_participants))))
    if mark_participated:
        added_to_event_participated_list = len(current_event_participants) - initial_len_part
        
    # Hosted list
    current_event_hosts = set(events_df.loc[idx, "Hosted"].split(',') if events_df.loc[idx, "Hosted"] else [])
    initial_len_host = len(current_event_hosts)
    if mark_hosted:
        current_event_hosts.update(employee_ids)
    events_df.loc[idx, "Hosted"] = ",".join(sorted(list(filter(None, current_event_hosts))))
    if mark_hosted:
        added_to_event_hosted_list = len(current_event_hosts) - initial_len_host


    # Update participants.csv
    new_participant_records = []
    for emp_id in employee_ids:
        emp_row = employees_df[employees_df["Standard ID"] == emp_id]
        if emp_row.empty:
            st.warning(f"Employee ID {emp_id} not found in employees table. Skipping participation record.")
            continue
        emp_email = emp_row["Email"].iloc[0]
        
        existing_record_indices = participants_df[
            (participants_df["Standard ID"] == emp_id) & 
            (participants_df["Event ID"] == event_id)
        ].index
        
        if existing_record_indices.empty:
            new_participant_records.append({
                "Standard ID": emp_id,
                "Email": emp_email,
                "Event ID": event_id,
                "Event Name": event_name,
                "Event Date": event_date.strftime("%Y-%m-%d") if isinstance(event_date, pd.Timestamp) else event_date,
                "Registered": "Yes" if mark_registered else "No",
                "Participated": "Yes" if mark_participated else "No",
                "Hosted": "Yes" if mark_hosted else "No",
                "Last Updated": current_time
            })
        else:
            record_idx_to_update = existing_record_indices[0]
            changed_in_existing = False
            if mark_registered and participants_df.loc[record_idx_to_update, "Registered"] != "Yes":
                participants_df.loc[record_idx_to_update, "Registered"] = "Yes"
                changed_in_existing = True
            
            if mark_participated and participants_df.loc[record_idx_to_update, "Participated"] != "Yes":
                participants_df.loc[record_idx_to_update, "Participated"] = "Yes"
                changed_in_existing = True
            
            if mark_hosted and participants_df.loc[record_idx_to_update, "Hosted"] != "Yes":
                participants_df.loc[record_idx_to_update, "Hosted"] = "Yes"
                changed_in_existing = True

            if changed_in_existing:
                 participants_df.loc[record_idx_to_update, "Last Updated"] = current_time

    if new_participant_records:
        new_records_df = pd.DataFrame(new_participant_records)
        participants_df = pd.concat([participants_df, new_records_df], ignore_index=True)

    save_table("events", events_df)
    save_table("participants", participants_df)
    load_table.clear()
    return added_to_event_registered_list, added_to_event_participated_list, added_to_event_hosted_list


def update_cohort_membership(cohort_name: str, employee_ids: list[str], mark_nominated: bool, mark_participant: bool) -> tuple[int, int]:
    """Adds employee IDs to the Nominated and/or Participants fields for a given cohort."""
    if not cohort_name or not employee_ids or (not mark_nominated and not mark_participant):
        return 0, 0 # Nothing to do

    cohorts_df = load_table("cohorts")
    load_table.clear() # Ensure we work with latest data

    cohort_index = cohorts_df.index[cohorts_df["Name"] == cohort_name].tolist()
    if not cohort_index:
        st.error(f"Cohort Name '{cohort_name}' not found.")
        return 0, 0

    idx = cohort_index[0]
    added_nominated = 0
    added_participants = 0

    # Update cohorts.csv
    if mark_nominated:
        current_nominated = set(cohorts_df.loc[idx, "Nominated"].split(',') if cohorts_df.loc[idx, "Nominated"] else [])
        initial_len = len(current_nominated)
        current_nominated.update(employee_ids)
        cohorts_df.loc[idx, "Nominated"] = ",".join(sorted(list(current_nominated)))
        added_nominated = len(current_nominated) - initial_len

    if mark_participant:
        current_participants = set(cohorts_df.loc[idx, "Participants"].split(',') if cohorts_df.loc[idx, "Participants"] else [])
        initial_len = len(current_participants)
        current_participants.update(employee_ids)
        cohorts_df.loc[idx, "Participants"] = ",".join(sorted(list(current_participants)))
        added_participants = len(current_participants) - initial_len

    save_table("cohorts", cohorts_df)
    load_table.clear() # Ensure cache is cleared after saving
    return added_nominated, added_participants


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

    employees_df = load_table("employees")
    events_df = load_table("events")
    participants_df = load_table("participants")

    if events_df.empty:
        st.warning("No events found. Please add events in the 'Events' section first.")
    elif employees_df.empty:
        st.warning("No employees found. Please add employees in the 'Employees' section first.")
    else:
        # Show existing participation records
        
        participants_df_loaded = load_table("participants")

        if not participants_df_loaded.empty:
            participants_df_for_editor = participants_df_loaded.copy()
            participants_df_for_editor["Registered"] = participants_df_for_editor.get("Registered", pd.Series(dtype=str)).apply(lambda x: True if x == "Yes" else False)
            participants_df_for_editor["Participated"] = participants_df_for_editor.get("Participated", pd.Series(dtype=str)).apply(lambda x: True if x == "Yes" else False)
            participants_df_for_editor["Hosted"] = participants_df_for_editor.get("Hosted", pd.Series(dtype=str)).apply(lambda x: True if x == "Yes" else False)

            column_config_participants = {
                "Standard ID": st.column_config.TextColumn("Standard ID", disabled=True),
                "Email": st.column_config.TextColumn("Email", disabled=True),
                "Event ID": st.column_config.TextColumn("Event ID", disabled=True),
                "Event Name": st.column_config.TextColumn("Event Name", disabled=True),
                "Event Date": st.column_config.DateColumn("Event Date", format="YYYY-MM-DD", disabled=True),
                "Registered": st.column_config.CheckboxColumn("Registered", default=False),
                "Participated": st.column_config.CheckboxColumn("Participated", default=False),
                "Hosted": st.column_config.CheckboxColumn("Hosted", default=False),
                "Last Updated": st.column_config.TextColumn("Last Updated", disabled=True)
            }

            edited_participants_with_bools = st.data_editor(
                participants_df_for_editor,
                num_rows="dynamic",
                key="editor_participants",
                use_container_width=True,
                column_config=column_config_participants,
            )

            if st.button("💾  Save", key="save_participants_changes"):
                current_participants_df_on_save = load_table("participants")
                current_events_df_on_save = load_table("events")
                
                participants_to_save_working_copy = current_participants_df_on_save.copy()
                events_to_save_working_copy = current_events_df_on_save.copy()
                
                made_changes_to_participants_file = False
                made_changes_to_events_file = False
                current_time_for_save = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                original_statuses_map = {}
                for _, orig_row in current_participants_df_on_save.iterrows():
                    original_statuses_map[(orig_row["Standard ID"], orig_row["Event ID"])] = {
                        "Registered": orig_row.get("Registered", "No"),
                        "Participated": orig_row.get("Participated", "No"),
                        "Hosted": orig_row.get("Hosted", "No")
                    }
                
                edited_rows_map = {}
                for _idx_edit, edited_row in edited_participants_with_bools.iterrows():
                    std_id = edited_row["Standard ID"]
                    evt_id = edited_row["Event ID"]
                    edited_rows_map[(std_id, evt_id)] = edited_row

                    new_reg_status_str = "Yes" if edited_row["Registered"] else "No"
                    new_part_status_str = "Yes" if edited_row["Participated"] else "No"
                    new_host_status_str = "Yes" if edited_row["Hosted"] else "No"

                    original_reg = original_statuses_map.get((std_id, evt_id), {}).get("Registered", "No")
                    original_part = original_statuses_map.get((std_id, evt_id), {}).get("Participated", "No")
                    original_host = original_statuses_map.get((std_id, evt_id), {}).get("Hosted", "No")

                    match_indices_participants = participants_to_save_working_copy[
                        (participants_to_save_working_copy["Standard ID"] == std_id) &
                        (participants_to_save_working_copy["Event ID"] == evt_id)
                    ].index

                    if not match_indices_participants.empty:
                        p_save_idx = match_indices_participants[0]
                        row_changed = False
                        if participants_to_save_working_copy.loc[p_save_idx, "Registered"] != new_reg_status_str:
                            participants_to_save_working_copy.loc[p_save_idx, "Registered"] = new_reg_status_str
                            row_changed = True
                        if participants_to_save_working_copy.loc[p_save_idx, "Participated"] != new_part_status_str:
                            participants_to_save_working_copy.loc[p_save_idx, "Participated"] = new_part_status_str
                            row_changed = True
                        if participants_to_save_working_copy.loc[p_save_idx, "Hosted"] != new_host_status_str:
                            participants_to_save_working_copy.loc[p_save_idx, "Hosted"] = new_host_status_str
                            row_changed = True
                        
                        if row_changed:
                            participants_to_save_working_copy.loc[p_save_idx, "Last Updated"] = current_time_for_save
                            made_changes_to_participants_file = True
                        # Check if only the timestamp needs updating due to a flip-flop back to original, but ensure it's marked for saving
                        elif (new_reg_status_str == original_reg and 
                              new_part_status_str == original_part and 
                              new_host_status_str == original_host and 
                              participants_to_save_working_copy.loc[p_save_idx, "Last Updated"] != current_time_for_save):
                            # This case is unlikely if row_changed already covers it, but added for completeness
                            # if the status flipped then flipped back, but we want to ensure last_updated is current if any interaction happened.
                            # However, the original logic already covers this by checking against original values before setting last_updated.
                            # The primary goal is: if any of [reg, part, host] differs from original_db_state, update timestamp.
                            # The current `if row_changed:` correctly captures any actual change from the loaded state to the edited state.
                            pass # The row_changed logic is sufficient.

                        # More precise check for timestamp update if values changed from their DB original state
                        if (new_reg_status_str != original_reg or \
                            new_part_status_str != original_part or \
                            new_host_status_str != original_host) and not row_changed:
                            # This condition handles if the editor values are different from DB originals,
                            # but the `participants_to_save_working_copy` already matched them (e.g. from a previous unsaved edit)
                            # and now the user is re-saving. We still want to update the timestamp.
                            # However, the `row_changed` flag already indicates `participants_to_save_working_copy` was modified.
                            # The most important logic is that `made_changes_to_participants_file` becomes True if any field changed OR if a new row.

                            pass # The current structure should be fine: `row_changed` triggers `made_changes_to_participants_file`

                        # Simpler logic: if any of the statuses changed from their original disk state, update timestamp
                        # This is implicitly handled if `row_changed` is true.
                        # The critical part is that `made_changes_to_participants_file` is True if `row_changed` is True.

                    else:
                        # This means a new row was added in the editor.
                        st.warning(f"Row for {std_id}/{evt_id} added in editor; full data may be missing if not in source.")
                        new_row_data = {
                            "Standard ID": std_id, "Event ID": evt_id,
                            "Email": employees_df[employees_df["Standard ID"] == std_id]["Email"].iloc[0] if not employees_df[employees_df["Standard ID"] == std_id].empty else "",
                            "Event Name": events_df[events_df["Event ID"] == evt_id]["Name"].iloc[0] if not events_df[events_df["Event ID"] == evt_id].empty else "",
                            "Event Date": events_df[events_df["Event ID"] == evt_id]["Date"].iloc[0] if not events_df[events_df["Event ID"] == evt_id].empty else pd.NaT,
                            "Registered": new_reg_status_str,
                            "Participated": new_part_status_str,
                            "Hosted": new_host_status_str,
                            "Last Updated": current_time_for_save
                        }
                        if pd.notna(new_row_data["Event Date"]) and isinstance(new_row_data["Event Date"], pd.Timestamp):
                           new_row_data["Event Date"] = new_row_data["Event Date"].strftime("%Y-%m-%d")
                        elif pd.isna(new_row_data["Event Date"]):
                            new_row_data["Event Date"] = ""

                        participants_to_save_working_copy = pd.concat([participants_to_save_working_copy, pd.DataFrame([new_row_data])], ignore_index=True)
                        made_changes_to_participants_file = True

                    event_match_indices_events = events_to_save_working_copy[events_to_save_working_copy["Event ID"] == evt_id].index
                    if not event_match_indices_events.empty:
                        e_save_idx = event_match_indices_events[0]
                        
                        current_event_regs = set(str(events_to_save_working_copy.loc[e_save_idx, "Registrations"]).split(',') if events_to_save_working_copy.loc[e_save_idx, "Registrations"] else [])
                        event_reg_changed = False
                        if new_reg_status_str == "Yes" and std_id not in current_event_regs:
                            current_event_regs.add(std_id)
                            event_reg_changed = True
                        elif new_reg_status_str == "No" and std_id in current_event_regs:
                            current_event_regs.remove(std_id)
                            event_reg_changed = True
                        if event_reg_changed:
                            events_to_save_working_copy.loc[e_save_idx, "Registrations"] = ",".join(sorted(list(filter(None, current_event_regs))))
                            made_changes_to_events_file = True

                        current_event_parts = set(str(events_to_save_working_copy.loc[e_save_idx, "Participants"]).split(',') if events_to_save_working_copy.loc[e_save_idx, "Participants"] else [])
                        event_part_changed = False
                        if new_part_status_str == "Yes" and std_id not in current_event_parts:
                            current_event_parts.add(std_id)
                            event_part_changed = True
                        elif new_part_status_str == "No" and std_id in current_event_parts:
                            current_event_parts.remove(std_id)
                            event_part_changed = True
                        if event_part_changed:
                            events_to_save_working_copy.loc[e_save_idx, "Participants"] = ",".join(sorted(list(filter(None, current_event_parts))))
                            made_changes_to_events_file = True

                        current_event_hosts_list = set(str(events_to_save_working_copy.loc[e_save_idx, "Hosted"]).split(',') if events_to_save_working_copy.loc[e_save_idx, "Hosted"] else [])
                        event_host_changed = False
                        if new_host_status_str == "Yes" and std_id not in current_event_hosts_list:
                            current_event_hosts_list.add(std_id)
                            event_host_changed = True
                        elif new_host_status_str == "No" and std_id in current_event_hosts_list:
                            current_event_hosts_list.remove(std_id)
                            event_host_changed = True
                        if event_host_changed:
                            events_to_save_working_copy.loc[e_save_idx, "Hosted"] = ",".join(sorted(list(filter(None, current_event_hosts_list))))
                            made_changes_to_events_file = True
                
                deleted_participant_keys = set(original_statuses_map.keys()) - set(edited_rows_map.keys())

                if deleted_participant_keys:
                    made_changes_to_participants_file = True
                    indices_to_drop_from_participants = []
                    
                    for p_orig_idx, p_orig_row in participants_to_save_working_copy.iterrows():
                        if (p_orig_row["Standard ID"], p_orig_row["Event ID"]) in deleted_participant_keys:
                            indices_to_drop_from_participants.append(p_orig_idx)
                    
                    if indices_to_drop_from_participants:
                         participants_to_save_working_copy = participants_to_save_working_copy.drop(indices_to_drop_from_participants).reset_index(drop=True)

                    for del_std_id, del_evt_id in deleted_participant_keys:
                        del_event_match_idx = events_to_save_working_copy[events_to_save_working_copy["Event ID"] == del_evt_id].index
                        if not del_event_match_idx.empty:
                            e_del_idx = del_event_match_idx[0]
                            
                            del_event_regs = set(str(events_to_save_working_copy.loc[e_del_idx, "Registrations"]).split(',') if events_to_save_working_copy.loc[e_del_idx, "Registrations"] else [])
                            if del_std_id in del_event_regs:
                                del_event_regs.remove(del_std_id)
                                events_to_save_working_copy.loc[e_del_idx, "Registrations"] = ",".join(sorted(list(filter(None, del_event_regs))))
                                made_changes_to_events_file = True
                            
                            del_event_parts = set(str(events_to_save_working_copy.loc[e_del_idx, "Participants"]).split(',') if events_to_save_working_copy.loc[e_del_idx, "Participants"] else [])
                            if del_std_id in del_event_parts:
                                del_event_parts.remove(del_std_id)
                                events_to_save_working_copy.loc[e_del_idx, "Participants"] = ",".join(sorted(list(filter(None, del_event_parts))))
                                made_changes_to_events_file = True
                                
                            del_event_hosts = set(str(events_to_save_working_copy.loc[e_del_idx, "Hosted"]).split(',') if events_to_save_working_copy.loc[e_del_idx, "Hosted"] else [])
                            if del_std_id in del_event_hosts:
                                del_event_hosts.remove(del_std_id)
                                events_to_save_working_copy.loc[e_del_idx, "Hosted"] = ",".join(sorted(list(filter(None, del_event_hosts))))
                                made_changes_to_events_file = True
                                
                if made_changes_to_participants_file:
                    save_table("participants", participants_to_save_working_copy)
                if made_changes_to_events_file:
                    save_table("events", events_to_save_working_copy)

                # Check if any changes were persisted to files
                if made_changes_to_participants_file or made_changes_to_events_file:
                    st.success("Participation changes saved successfully!")
                    load_table.clear() 
                    st.rerun()
                else:
                    st.info("No changes detected in participation records.")
        else:
            st.info("No participation records found. Add them using the sidebar.")


        # Move participant management to sidebar
        with st.sidebar.expander("📝 Add/Update Participation Records", expanded=True):
            st.markdown("### Add New Participation Records")
            
            # Event Selection
            event_options = {f"{row['Event ID']} - {row['Name']} ({row['Date'].strftime('%Y-%m-%d')})": row['Event ID']
                             for _, row in events_df.sort_values("Date", ascending=False).iterrows()}
            selected_event_display = st.selectbox(
                "Select Event",
                options=list(event_options.keys())
            )
            selected_event_id = event_options.get(selected_event_display)

            st.divider()

            # Input Employee IDs/Emails
            st.markdown("#### Select Employees")
            employee_ids_to_process = []
            invalid_inputs_detected = []

            tab_paste, tab_select, tab_upload = st.tabs(["Paste List", "Select from List", "Upload File"])

            with tab_paste:
                pasted_list = st.text_area("Paste Standard IDs or Emails (one per line)", key="participants_paste")
                if pasted_list:
                    employee_ids_to_process, invalid_inputs_detected = get_employee_ids_from_input(pasted_list, employees_df)
                    st.write(f"Found {len(employee_ids_to_process)} valid employee(s).")
                    if invalid_inputs_detected:
                        st.warning(f"Could not find/validate: {', '.join(invalid_inputs_detected)}")

            with tab_select:
                employee_display_options = [f"{row['Standard ID']} - {row['Email']}"
                                            for _, row in employees_df.iterrows()]
                selected_employees = st.multiselect(
                    "Select Employees",
                    options=employee_display_options,
                    key="participants_multiselect"
                )
                if selected_employees:
                    employee_ids_to_process = [opt.split(' - ')[0] for opt in selected_employees]

            with tab_upload:
                uploaded_file = st.file_uploader("Upload .txt or .csv (one ID/email per line)", type=["txt", "csv"], key="participants_upload")
                if uploaded_file is not None:
                    stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                    file_content = stringio.read()
                    employee_ids_to_process, invalid_inputs_detected = get_employee_ids_from_input(file_content, employees_df)
                    st.write(f"Found {len(employee_ids_to_process)} valid employee(s).")
                    if invalid_inputs_detected:
                        st.warning(f"Could not find/validate: {', '.join(invalid_inputs_detected)}")

            st.divider()

            # Select Participation Type
            st.markdown("#### Set Participation Status")
            mark_registered = st.checkbox("Registered")
            mark_participated = st.checkbox("Participated")
            mark_hosted = st.checkbox("Hosted")

            st.divider()

            # Update Button
            if st.button("Update Participation", disabled=(not selected_event_id or not employee_ids_to_process or (not mark_registered and not mark_participated and not mark_hosted))):
                added_reg, added_part, added_host = update_event_participation(
                    selected_event_id,
                    employee_ids_to_process,
                    mark_registered,
                    mark_participated,
                    mark_hosted
                )
                success_msgs = []
                if mark_registered:
                     success_msgs.append(f"Added {added_reg} new registration(s).")
                if mark_participated:
                    success_msgs.append(f"Added {added_part} new participation record(s).")
                if mark_hosted:
                    success_msgs.append(f"Marked {added_host} as host(s).")

                if success_msgs:
                    st.success(f"Successfully updated event '{selected_event_display}'. {' '.join(success_msgs)}")
                    st.rerun()
                else:
                    st.info("No changes made (employees might already have the selected status).")

# --- Other Sections (Employees, Workshops, Cohorts, Events) ---
else:
    if table_key == "events":
        load_table.clear() # Ensure fresh load for events, especially after CSV changes
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
                        "Hosted": "",  # Initially empty, will be populated via Manage Participation
                        "Registrations": "", "Participants": ""
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
            "Participants": st.column_config.TextColumn("Participants", help="Comma-separated Standard IDs.", disabled=True),
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
                            "Participants": ""
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
                    selected_employees = st.multiselect(
                        "Select Employees",
                        options=employee_display_options,
                        key="cohort_multiselect"
                    )
                    if selected_employees:
                        employee_ids_to_process = [opt.split(' - ')[0] for opt in selected_employees]

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
                mark_participant = st.checkbox("Participated")

                st.divider()

                # Update Button
                if st.button("Update Cohort Membership", disabled=(not selected_cohort_name or not employee_ids_to_process or (not mark_nominated and not mark_participant))):
                    added_nom, added_part = update_cohort_membership(
                        selected_cohort_name,
                        employee_ids_to_process,
                        mark_nominated,
                        mark_participant
                    )
                    success_msgs = []
                    if mark_nominated:
                         success_msgs.append(f"Added {added_nom} new nomination(s).")
                    if mark_participant:
                        success_msgs.append(f"Added {added_part} new participant(s).")

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
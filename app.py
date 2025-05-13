import os
from datetime import datetime
import pandas as pd
import streamlit as st
import io # Needed for file uploads

###############################################################################
# Configuration
###############################################################################
DATA_DIR = "data"              # Folder where CSV files live (created if needed)

# Each logical table maps to a CSV file and a list of its canonical columns
FILES = {
    "employees": ("employees.csv", [
        "Standard ID", "Email", "Location", "Job Title",
        "L1 Department", "L2 Department", "L3 Department",
        "L4 Department", "L5 Department", "L6 Department",
        "L1 Manager", "L2 Manager", "L3 Manager",
        "L4 Manager", "L5 Manager", "L6 Manager",
        "Categories"       # Semi-colon-delimited list of special roles
    ]),
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
        "Registrations",    # Comma-separated Standard IDs
        "Participants"      # Comma-separated Standard IDs
    ]),
    "participants": ("participants.csv", [
        "Standard ID", "Email", "Event ID", "Event Name", "Event Date",
        "Status",           # Registered, Participated, or both
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
    """Make sure the data directory exists."""
    os.makedirs(DATA_DIR, exist_ok=True)


def _path_for(key: str) -> str:
    """Absolute CSV path for a given logical table key."""
    filename, _ = FILES[key]
    return os.path.join(DATA_DIR, filename)


@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_table(key: str) -> pd.DataFrame:
    """Load the CSV for *key* – if missing, create an empty one first."""
    path = _path_for(key)
    cols = FILES[key][1]

    if os.path.exists(path):
        df = pd.read_csv(
            path,
            dtype=str, # Initially read all as string
            na_filter=False,
            low_memory=True,
            engine='c'
        ).fillna("")

        # Special handling for date columns
        if key == "events" and "Date" in df.columns:
            # Attempt to convert 'Date' to datetime, coercing errors to NaT
            df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
        elif key == "cohorts" and "Date Started" in df.columns:
            # Convert 'Date Started' to datetime
            df["Date Started"] = pd.to_datetime(df["Date Started"], errors='coerce')

    else:
        df = pd.DataFrame(columns=cols)
        # If creating a new table, ensure date columns are datetime type
        if key == "events" and "Date" in cols:
            df["Date"] = pd.Series(dtype='datetime64[ns]')
        elif key == "cohorts" and "Date Started" in cols:
            df["Date Started"] = pd.Series(dtype='datetime64[ns]')
        df.to_csv(path, index=False)

    # Ensure all expected columns exist
    for col in cols:
        if col not in df.columns:
            df[col] = "" # Initialize new string columns
            if key == "events" and col == "Date" and "Date" not in df.columns:
                df["Date"] = pd.Series(dtype='datetime64[ns]')
            elif key == "cohorts" and col == "Date Started" and "Date Started" not in df.columns:
                df["Date Started"] = pd.Series(dtype='datetime64[ns]')

    # Re-ensure date columns are datetime if they were re-added as string
    if key == "events" and "Date" in df.columns and df["Date"].dtype == object:
        df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    elif key == "cohorts" and "Date Started" in df.columns and df["Date Started"].dtype == object:
        df["Date Started"] = pd.to_datetime(df["Date Started"], errors='coerce')

    return df[cols]   # Enforce column order


def save_table(key: str, df: pd.DataFrame) -> None:
    """Persist *df* back to disk for logical table *key*."""
    path = _path_for(key)
    df.to_csv(path, index=False)


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


def update_event_participation(event_id: str, employee_ids: list[str], mark_registered: bool, mark_participated: bool) -> tuple[int, int]:
    """Adds employee IDs to the Registrations and/or Participants fields for a given event."""
    if not event_id or not employee_ids or (not mark_registered and not mark_participated):
        return 0, 0 # Nothing to do

    events_df = load_table("events")
    employees_df = load_table("employees")
    participants_df = load_table("participants")
    load_table.clear() # Ensure we work with latest data

    event_index = events_df.index[events_df["Event ID"] == event_id].tolist()
    if not event_index:
        st.error(f"Event ID {event_id} not found.")
        return 0, 0

    idx = event_index[0]
    event_name = events_df.loc[idx, "Name"]
    event_date = events_df.loc[idx, "Date"]
    added_registered = 0
    added_participated = 0
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Update events.csv
    if mark_registered:
        current_registered = set(events_df.loc[idx, "Registrations"].split(',') if events_df.loc[idx, "Registrations"] else [])
        initial_len = len(current_registered)
        current_registered.update(employee_ids)
        events_df.loc[idx, "Registrations"] = ",".join(sorted(list(current_registered)))
        added_registered = len(current_registered) - initial_len

    if mark_participated:
        current_participated = set(events_df.loc[idx, "Participants"].split(',') if events_df.loc[idx, "Participants"] else [])
        initial_len = len(current_participated)
        current_participated.update(employee_ids)
        events_df.loc[idx, "Participants"] = ",".join(sorted(list(current_participated)))
        added_participated = len(current_participated) - initial_len

    # Update participants.csv
    new_participant_records = []
    for emp_id in employee_ids:
        emp_email = employees_df.loc[employees_df["Standard ID"] == emp_id, "Email"].iloc[0]
        status = []
        if mark_registered:
            status.append("Registered")
        if mark_participated:
            status.append("Participated")
        
        # Check if record exists
        existing_record = participants_df[
            (participants_df["Standard ID"] == emp_id) & 
            (participants_df["Event ID"] == event_id)
        ]
        
        if existing_record.empty:
            # Add new record
            new_participant_records.append({
                "Standard ID": emp_id,
                "Email": emp_email,
                "Event ID": event_id,
                "Event Name": event_name,
                "Event Date": event_date.strftime("%Y-%m-%d") if isinstance(event_date, pd.Timestamp) else event_date,
                "Status": ", ".join(status),
                "Last Updated": current_time
            })
        else:
            # Update existing record
            participants_df.loc[existing_record.index, "Status"] = ", ".join(status)
            participants_df.loc[existing_record.index, "Last Updated"] = current_time

    if new_participant_records:
        new_records_df = pd.DataFrame(new_participant_records)
        participants_df = pd.concat([participants_df, new_records_df], ignore_index=True)

    save_table("events", events_df)
    save_table("participants", participants_df)
    load_table.clear() # Ensure cache is cleared after saving
    return added_registered, added_participated


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

SECTION_NAMES = {
    "Employees": "employees",
    "Workshop Series": "workshops",
    "Cohorts": "cohorts",
    "Events": "events",
    "Participants": "manage_participation" # Changed display name, kept internal key
}
# Use index=3 to default to Events view if possible, else 0
default_index = 3 if len(SECTION_NAMES) > 3 else 0
section_label = st.sidebar.selectbox("Section", list(SECTION_NAMES.keys()), index=default_index)
table_key = SECTION_NAMES[section_label]

# ---------------------------------------------------------------------------
# Load data & editor / section display
# ---------------------------------------------------------------------------

# --- Manage Participation Section ---
if table_key == "manage_participation":
    st.subheader("Participants")

    employees_df = load_table("employees")
    events_df = load_table("events")
    participants_df = load_table("participants")

    if events_df.empty:
        st.warning("No events found. Please add events in the 'Events' section first.")
    elif employees_df.empty:
        st.warning("No employees found. Please add employees in the 'Employees' section first.")
    else:
        # Show existing participation records
        st.markdown("### Participation Records")
        if not participants_df.empty:
            # Configure the data editor for participants
            column_config_participants = {
                "Standard ID": st.column_config.TextColumn("Standard ID", disabled=True),
                "Email": st.column_config.TextColumn("Email", disabled=True),
                "Event ID": st.column_config.TextColumn("Event ID", disabled=True),
                "Event Name": st.column_config.TextColumn("Event Name", disabled=True),
                "Event Date": st.column_config.DateColumn("Event Date", format="YYYY-MM-DD", disabled=True),
                "Status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["Registered", "Participated", "Registered, Participated"],
                    required=True
                ),
                "Last Updated": st.column_config.TextColumn("Last Updated", disabled=True)
            }

            edited_participants = st.data_editor(
                participants_df,
                num_rows="dynamic",
                key="editor_participants",
                use_container_width=True,
                column_config=column_config_participants
            )

            if st.button("💾 Save Changes", key="save_participants"):
                # Update both participants.csv and events.csv
                for _, row in edited_participants.iterrows():
                    statuses = row["Status"].split(", ")
                    update_event_participation(
                        row["Event ID"],
                        [row["Standard ID"]],
                        "Registered" in statuses,
                        "Participated" in statuses
                    )
                st.success("Changes saved successfully!")
                st.rerun()

        st.markdown("---")

        # Add new participation records
        st.markdown("### Add New Participation Records")
        
        # Event Selection
        event_options = {f"{row['Event ID']} - {row['Name']} ({row['Date'].strftime('%Y-%m-%d')})": row['Event ID']
                         for _, row in events_df.sort_values("Date", ascending=False).iterrows()}
        selected_event_display = st.selectbox(
            "Select Event",
            options=list(event_options.keys())
        )
        selected_event_id = event_options.get(selected_event_display)

        st.markdown("---")

        # Input Employee IDs/Emails
        st.markdown("#### Select Employees")
        input_method = st.tabs(["Paste List", "Select from List", "Upload File"])
        employee_ids_to_process = []
        invalid_inputs_detected = []

        with input_method[0]: # Paste List
            pasted_list = st.text_area("Paste Standard IDs or Emails (one per line)", key="participants_paste")
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
                key="participants_multiselect"
            )
            if selected_employees:
                employee_ids_to_process = [opt.split(' - ')[0] for opt in selected_employees]

        with input_method[2]: # Upload File
            uploaded_file = st.file_uploader("Upload .txt or .csv (one ID/email per line)", type=["txt", "csv"], key="participants_upload")
            if uploaded_file is not None:
                stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                file_content = stringio.read()
                employee_ids_to_process, invalid_inputs_detected = get_employee_ids_from_input(file_content, employees_df)
                st.write(f"Found {len(employee_ids_to_process)} valid employee(s).")
                if invalid_inputs_detected:
                    st.warning(f"Could not find/validate: {', '.join(invalid_inputs_detected)}")

        st.markdown("---")

        # Select Participation Type
        st.markdown("#### Set Participation Status")
        mark_registered = st.checkbox("Registered")
        mark_participated = st.checkbox("Participated")

        st.markdown("---")

        # Update Button
        if st.button("Update Participation", disabled=(not selected_event_id or not employee_ids_to_process or (not mark_registered and not mark_participated))):
            added_reg, added_part = update_event_participation(
                selected_event_id,
                employee_ids_to_process,
                mark_registered,
                mark_participated
            )
            success_msgs = []
            if mark_registered:
                 success_msgs.append(f"Added {added_reg} new registration(s).")
            if mark_participated:
                success_msgs.append(f"Added {added_part} new participation record(s).")

            if success_msgs:
                st.success(f"Successfully updated event '{selected_event_display}'. {' '.join(success_msgs)}")
                st.rerun()
            else:
                st.info("No changes made (employees might already have the selected status).")

# --- Other Sections (Employees, Workshops, Cohorts, Events) ---
else:
    df = load_table(table_key)
    st.subheader(f"{section_label} Table")

    # Pagination for large datasets
    df_display = df # Default to full display
    if len(df) > 1000:
        page_size = st.sidebar.slider("Rows per page", 100, 1000, 500, key=f"pagesize_{table_key}")
        total_pages = len(df) // page_size + (1 if len(df) % page_size > 0 else 0)
        page = st.sidebar.number_input("Page", 1, total_pages, 1, key=f"page_{table_key}")
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        df_display = df.iloc[start_idx:end_idx]
        st.sidebar.caption(f"Showing {start_idx + 1}-{min(end_idx, len(df))} of {len(df)} rows")

    # --- Specific Editor Configurations ---

    # Custom editor for Events section
    if table_key == "events":
        # Prepare column configurations for Events data_editor
        workshop_df_for_options = load_table("workshops")
        workshop_select_options = [f"{row['Workshop #']} - {row['Skill']}: {row['Goal']}"
                                   for _, row in workshop_df_for_options.iterrows()]
        workshop_select_options.insert(0, "") # Blank option

        column_config_events = {
            "Workshop": st.column_config.SelectboxColumn(
                "Workshop", help="Select the workshop this event is an instance of.",
                options=workshop_select_options, required=False
            ),
            "Registrations": st.column_config.TextColumn("Registrations", help="Employee IDs registered (updated via Manage Participation).", disabled=True),
            "Participants": st.column_config.TextColumn("Participants", help="Employee IDs participated (updated via Manage Participation).", disabled=True),
            "Event ID": st.column_config.TextColumn("Event ID", disabled=True),
            "Date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            "Category": st.column_config.SelectboxColumn("Category", options=list(EVENT_CATEGORIES.keys()), required=True)
        }

        st.markdown("### Events Table")
        edited_df = st.data_editor(
            df_display, num_rows="dynamic", key=f"editor_{table_key}",
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

        # Form for adding new events
        st.markdown("### Add New Event")
        with st.form("new_event_form"):
            col1, col2 = st.columns(2)
            with col1:
                event_name = st.text_input("Event Name")
                event_date = st.date_input("Event Date")
                event_category = st.selectbox("Category", options=list(EVENT_CATEGORIES.keys()), help="Select the type of event")
            with col2:
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
                    "Event ID": event_id, "Name": event_name, "Date": event_date.strftime("%Y-%m-%d"),
                    "Category": event_category, "Workshop": selected_workshop_id,
                    "Registrations": "", "Participants": ""
                }])
                st.session_state['newly_added_event'] = new_event
                st.rerun()

        # Save Button for Table Edits
        st.markdown("### Save Table Edits")
        if st.button("💾 Save Table Changes", key=f"save_{table_key}"):
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
        st.subheader("Cohorts")
        employees_df = load_table("employees") # Needed for employee selection

        # Display existing cohorts
        st.markdown("### Cohorts Table")
        column_config_cohorts = {
            "Nominated": st.column_config.TextColumn("Nominated", help="Comma-separated Standard IDs.", disabled=True),
            "Participants": st.column_config.TextColumn("Participants", help="Comma-separated Standard IDs.", disabled=True),
            "Date Started": st.column_config.DateColumn("Date Started", format="YYYY-MM-DD", required=True)
        }
        edited_df = st.data_editor(
            df_display, num_rows="dynamic", key=f"editor_{table_key}",
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
        st.markdown("### Save Table Edits")
        if st.button("💾 Save Table Changes", key=f"save_{table_key}"):
             if len(df) > 1000:
                df.iloc[start_idx:end_idx] = edited_df
                save_table(table_key, df)
             else:
                 save_table(table_key, edited_df) # Save directly if not paginated
             st.success("Table changes saved to disk.")
             load_table.clear()
             st.rerun()

        st.markdown("---")

        # Form for adding new cohorts
        st.markdown("### Add New Cohort")
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
                        "Date Started": cohort_date.strftime("%Y-%m-%d"),
                        "Nominated": "",
                        "Participants": ""
                    }])
                    st.session_state['newly_added_cohort'] = new_cohort
                    st.rerun()
            elif submitted:
                st.warning("Cohort Name cannot be empty.")

        st.markdown("---")

        # Manage Cohort Membership
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

            st.markdown("---")

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

            st.markdown("---")

            # Select Membership Type
            st.markdown("#### Set Membership Status")
            mark_nominated = st.checkbox("Nominated")
            mark_participant = st.checkbox("Participated")

            st.markdown("---")

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
        edited_df = st.data_editor(
            df_display, num_rows="dynamic", key=f"editor_{table_key}",
            use_container_width=True
        )
        if st.button("💾 Save changes", key=f"save_{table_key}"):
            if len(df) > 1000:
                df.iloc[start_idx:end_idx] = edited_df
            else:
                df = edited_df # Update the main df directly if not paginated
            save_table(table_key, df)
            st.success("Saved to disk.")
            load_table.clear() # Clear cache after saving
            st.rerun()


    # ---------------------------------------------------------------------------
    # Context-specific helpers (Only for non-participation sections)
    # ---------------------------------------------------------------------------
    if table_key == "employees":
        st.markdown("### Quick Category Tagger")
        if not df.empty:
            # Use the full dataframe 'df' for selection, not 'df_display'
            employee_options = {f"{row['Standard ID']} - {row['Email']}": row['Standard ID']
                                for _, row in df.iterrows()}
            selected_employee_display = st.selectbox(
                "Choose employee",
                options=list(employee_options.keys())
            )
            employee_id = employee_options.get(selected_employee_display)

            if employee_id:
                categories_master = ["Working Group Lead", "Train-the-Trainer Candidate", "AInfluencer", "Offering Support"]
                current_cats_series = df.loc[df["Standard ID"] == employee_id, "Categories"]
                current = current_cats_series.iloc[0] if not current_cats_series.empty else ""
                current_list = [c for c in current.split(";") if c]

                new_cats = st.multiselect("Assign special roles", categories_master, default=current_list, key=f"multiselect_{employee_id}")
                if st.button("Update categories"):
                    df.loc[df["Standard ID"] == employee_id, "Categories"] = ";".join(new_cats)
                    save_table(table_key, df)
                    st.success("Categories updated.")
                    load_table.clear() # Clear cache
                    st.rerun()

    elif table_key == "workshops":
        st.info(
            "Use **Instances** to store comma-separated Event IDs. "
            "**Registered/Participated** store comma-separated Standard IDs.\n\n"
            "Relations are simple CSV references; production apps need a database."
        )

    elif table_key == "cohorts":
        st.info("Nominated & Participants expect comma-separated Standard IDs.")

    elif table_key == "events":
        st.markdown("""⚙️ **Guidance**  
     <ul>
     <li>Set <code>Category</code> to <em>Workshop</em> and enter the corresponding <code>Workshop #</code> if this event is an official workshop instance.</li>
     <li>Keep <code>Date</code> in YYYY-MM-DD format for easy sorting.</li>
     </ul>""", unsafe_allow_html=True)

###############################################################################
# Footer
###############################################################################
st.sidebar.markdown("---")
st.sidebar.caption(
    "CSV-backed prototype stored in the ./data folder."
)
st.sidebar.caption("Built with Streamlit • pandas • Python ≥ 3.9")
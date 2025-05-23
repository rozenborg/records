import streamlit as st
import pandas as pd
import io
from typing import List, Tuple


def _parse_employee_identifiers(raw_text: str, employees_df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """Given a raw multiline string of IDs or emails, validate them against *employees_df*.
    Handles both line-by-line and comma-separated formats.

    Returns:
        Tuple[List[str], List[str]]: 
            - all_identifiers_to_use: Identifiers for processing. (Standard ID if found, original input if not).
            - inputs_not_found_in_employees: Original inputs not found in employees_df.
    """
    # First split by newlines, then by commas, and flatten the list
    raw_items = []
    for line in raw_text.strip().split('\n'):
        # Split by comma and strip whitespace from each item
        items = [item.strip() for item in line.split(',')]
        raw_items.extend(items)
    
    # Remove empty strings
    raw_items = [item for item in raw_items if item]
    
    if not raw_items:
        return [], []

    email_to_id = employees_df.set_index("Email")["Standard ID"] if "Email" in employees_df.columns and "Standard ID" in employees_df.columns and not employees_df.empty else pd.Series(dtype=str)
    id_set = set(employees_df["Standard ID"]) if "Standard ID" in employees_df.columns and not employees_df.empty else set()

    all_identifiers_to_use = []
    inputs_not_found_in_employees = []

    for identifier in raw_items:
        is_email = "@" in identifier
        if is_email:
            std_id = email_to_id.get(identifier)
            if std_id:  # Email found
                all_identifiers_to_use.append(std_id)
            else:  # Email not found
                all_identifiers_to_use.append(identifier)  # Use the email itself for processing
                inputs_not_found_in_employees.append(identifier)
        else:  # Treat as Standard ID
            all_identifiers_to_use.append(identifier)  # Use the ID itself for processing
            if identifier not in id_set:  # Standard ID not found
                inputs_not_found_in_employees.append(identifier)
    
    return all_identifiers_to_use, inputs_not_found_in_employees


def employee_selector(employees_df: pd.DataFrame, *, key_prefix: str = "") -> Tuple[List[str], List[str]]:
    """Reusable UI component that lets the user select one or more employees.

    The component offers:
    1.  A free-text area where the user can paste IDs/emails
    2.  A multiselect list of all employees
    3.  An upload widget for a .txt or .csv file containing IDs/emails

    Returns:
        Tuple[List[str], List[str]]:
            - unique_identifiers_for_processing: Deduplicated list of (Standard IDs where found, or original input if not found).
            - unique_identifiers_not_in_employees: Deduplicated list of original inputs not found in employees_df.
    """
    if employees_df.empty:
        st.warning("No employees found in the Employees table. Any identifiers entered will be treated as new/unvalidated.")
        # Allow proceeding even if employees_df is empty

    container = st.container()
    
    all_collected_ids_for_processing = []
    all_collected_ids_not_in_employees = []

    with container:
        tab_paste, tab_select, tab_upload = st.tabs(["Paste List", "Select from List", "Upload File"])

        # ---- Paste List tab ----
        with tab_paste:
            pasted_text = st.text_area(
                "Paste Standard IDs or Emails",
                key=f"{key_prefix}_paste",
            )
            if pasted_text:
                ids_proc, ids_not_found = _parse_employee_identifiers(pasted_text, employees_df)
                all_collected_ids_for_processing.extend(ids_proc)
                all_collected_ids_not_in_employees.extend(ids_not_found)
                if ids_proc:
                    st.write(f"Parsed {len(ids_proc)} identifier(s) from pasted text.")
                if ids_not_found:
                    st.warning(f"The following {len(ids_not_found)} identifier(s) from pasted text were not found in the Employees table but will be included: {', '.join(ids_not_found)}")

        # ---- Select from List tab ----
        with tab_select:
            if not employees_df.empty:
                employee_display_options = [
                    f"{row['Standard ID']} - {row['Email']}" for _, row in employees_df.iterrows()
                ]
                selected_opts = st.multiselect(
                    "Select Employees",
                    options=employee_display_options,
                    key=f"{key_prefix}_multiselect",
                )
                if selected_opts:
                    ids_from_select = [opt.split(" - ")[0] for opt in selected_opts]
                    all_collected_ids_for_processing.extend(ids_from_select)
                    # IDs from multiselect are by definition "found", so nothing for all_collected_ids_not_in_employees
            else:
                st.info("Employee list is empty, selection disabled.")


        # ---- Upload File tab ----
        with tab_upload:
            uploaded_file = st.file_uploader(
                "Upload .txt or .csv (one ID/email per line)",
                type=["txt", "csv"],
                key=f"{key_prefix}_upload",
            )
            if uploaded_file is not None:
                try:
                    stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
                    file_content = stringio.read()
                    ids_proc_file, ids_not_found_file = _parse_employee_identifiers(
                        file_content, employees_df
                    )
                    all_collected_ids_for_processing.extend(ids_proc_file)
                    all_collected_ids_not_in_employees.extend(ids_not_found_file)

                    if ids_proc_file:
                        st.write(f"Parsed {len(ids_proc_file)} identifier(s) from file.")
                    if ids_not_found_file:
                        st.warning(
                            f"The following {len(ids_not_found_file)} identifier(s) from file were not found in the Employees table but will be included: {', '.join(ids_not_found_file)}"
                        )
                except Exception as exc:
                    st.error(f"Failed to read uploaded file: {exc}")

    # Deduplicate while preserving order of first appearance
    seen_processing = set()
    unique_processing_ids = []
    for eid in all_collected_ids_for_processing:
        if eid not in seen_processing:
            seen_processing.add(eid)
            unique_processing_ids.append(eid)

    seen_not_found = set()
    unique_not_found_ids = []
    for eid_nf in all_collected_ids_not_in_employees:
        if eid_nf not in seen_not_found:
            seen_not_found.add(eid_nf)
            unique_not_found_ids.append(eid_nf)
    
    if unique_processing_ids:
        st.write(f"**Total unique identifiers selected/entered for processing:** {len(unique_processing_ids)}")
    
    return unique_processing_ids, unique_not_found_ids


def _parse_emails_from_text_area(raw_text: str) -> Tuple[List[str], List[str]]:
    """
    Parses a multiline/comma-separated string of emails.
    Returns:
        Tuple[List[str], List[str]]: (valid_emails, invalid_entries)
    """
    raw_items = []
    for line in raw_text.strip().split('\\n'):
        items = [item.strip() for item in line.split(',')]
        raw_items.extend(items)
    
    raw_items = [item for item in raw_items if item] # Remove empty strings
    
    valid_emails = []
    invalid_entries = []
    if not raw_items:
        return [], []

    for item in raw_items:
        if "@" in item: # Basic validation
            valid_emails.append(item)
        else:
            invalid_entries.append(item)
    return valid_emails, invalid_entries


def nominator_selector(employees_df: pd.DataFrame, *, key_prefix: str = "") -> List[str]:
    """
    UI component to select nominators using tabs: select from a list of employees or enter emails manually.
    Returns a deduplicated list of email strings.
    """
    all_emails_collected = []
    
    tab_select, tab_paste = st.tabs(["Select from Employee List", "Paste/Enter Emails Manually"])

    with tab_select:
        if not employees_df.empty and "Work Email Address" in employees_df.columns and not employees_df["Work Email Address"].dropna().empty:
            employee_email_options = sorted(list(employees_df["Work Email Address"].dropna().unique()))
            if employee_email_options: # Ensure there are actual emails to select
                selected_emails_from_list = st.multiselect(
                    "Select Nominator(s) from Employee List",
                    options=employee_email_options,
                    key=f"{key_prefix}_nominator_multiselect_tab", 
                    help="Select from existing employee emails."
                )
                all_emails_collected.extend(selected_emails_from_list)
            else:
                st.info("No emails found in the employee list to select from.")
        else:
            st.info("Employee data is unavailable or lacks emails for selection. Use the 'Paste/Enter Emails' tab.")

    with tab_paste:
        pasted_emails_text = st.text_area(
            "Enter or Paste Nominator Email(s)",
            key=f"{key_prefix}_nominator_paste_tab", 
            help="Enter emails separated by commas or new lines. These can be emails not in the employee list."
        )
        if pasted_emails_text:
            parsed_valid_emails, parsed_invalid_entries = _parse_emails_from_text_area(pasted_emails_text)
            all_emails_collected.extend(parsed_valid_emails)
            if parsed_invalid_entries:
                st.warning(f"The following entries from the text area were not recognized as valid emails and will be ignored: {', '.join(parsed_invalid_entries)}")

    # Deduplicate while preserving order of first appearance
    seen_emails = set()
    unique_emails = []
    for email in all_emails_collected:
        if email not in seen_emails:
            seen_emails.add(email)
            unique_emails.append(email)
            
    return unique_emails 
import streamlit as st
import pandas as pd
import io
from typing import List, Tuple


def _parse_employee_identifiers(raw_text: str, employees_df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """Given a raw multiline string of IDs or emails, validate them against *employees_df*.

    Returns (valid_ids, invalid_inputs).
    """
    raw_items = [item.strip() for item in raw_text.strip().split('\n') if item.strip()]
    if not raw_items:
        return [], []

    email_to_id = employees_df.set_index("Email")["Standard ID"] if "Email" in employees_df.columns else pd.Series(dtype=str)
    id_set = set(employees_df["Standard ID"]) if "Standard ID" in employees_df.columns else set()

    valid_ids = []
    invalid_inputs = []
    for identifier in raw_items:
        if "@" in identifier:  # Treat as email
            std_id = email_to_id.get(identifier)
            if std_id:
                valid_ids.append(std_id)
            else:
                invalid_inputs.append(identifier)
        else:  # Treat as Standard ID
            if identifier in id_set:
                valid_ids.append(identifier)
            else:
                invalid_inputs.append(identifier)

    # Deduplicate while preserving order
    seen = set()
    valid_ids_dedup = []
    for v in valid_ids:
        if v not in seen:
            seen.add(v)
            valid_ids_dedup.append(v)

    return valid_ids_dedup, invalid_inputs


def employee_selector(employees_df: pd.DataFrame, *, key_prefix: str = "") -> List[str]:
    """Reusable UI component that lets the user select one or more employees.

    The component offers:
    1.  A free-text area where the user can paste IDs/emails (one per line)
    2.  A multiselect list of all employees
    3.  An upload widget for a .txt or .csv file containing IDs/emails

    It returns a list of **Standard IDs**.
    """
    if employees_df.empty:
        st.warning("No employees found. Please add employees in the 'Employees' section first.")
        return []

    container = st.container()
    with container:
        tab_paste, tab_select, tab_upload = st.tabs(["Paste List", "Select from List", "Upload File"])

        collected_ids = []

        # ---- Paste List tab ----
        with tab_paste:
            pasted_text = st.text_area(
                "Paste Standard IDs or Emails (one per line)",
                key=f"{key_prefix}_paste",
            )
            if pasted_text:
                valid_ids, invalid_inputs = _parse_employee_identifiers(pasted_text, employees_df)
                if valid_ids:
                    st.write(f"Found {len(valid_ids)} valid employee(s).")
                    collected_ids.extend(valid_ids)
                if invalid_inputs:
                    st.warning(f"Could not find/validate: {', '.join(invalid_inputs)}")

        # ---- Select from List tab ----
        with tab_select:
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
                collected_ids.extend(ids_from_select)

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
                    valid_ids_file, invalid_inputs_file = _parse_employee_identifiers(
                        file_content, employees_df
                    )
                    if valid_ids_file:
                        st.write(f"Found {len(valid_ids_file)} valid employee(s) from file.")
                        collected_ids.extend(valid_ids_file)
                    if invalid_inputs_file:
                        st.warning(
                            f"Could not find/validate from file: {', '.join(invalid_inputs_file)}"
                        )
                except Exception as exc:
                    st.error(f"Failed to read uploaded file: {exc}")

    # Deduplicate while preserving order of first appearance
    seen_final = set()
    unique_ids = []
    for eid in collected_ids:
        if eid not in seen_final:
            seen_final.add(eid)
            unique_ids.append(eid)

    if unique_ids:
        st.write(f"**Total unique employees selected:** {len(unique_ids)}")

    return unique_ids 
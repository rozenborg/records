# Program Participation Tracker

A Streamlit-based application to manage and track participation in various programs, workshops, and events. This tool helps program administrators organize workshops, events, cohorts, and track employee participation across different initiatives.

## Features

- **Employees Management**: Store and categorize employee information. Assumes 'Standard ID' and 'Work Email Address' columns in your CSV. Other columns are loaded dynamically.
- **Workshop Series**: Define workshop series with skills and goals
- **Events Tracking**: Schedule and manage various event types (workshops, demos, meetings, conferences)
- **Cohorts Management**: Create and manage cohorts of employees
- **Participation Tracking**: Record and track employee registration and participation in events
- **Flexible Column Display**: Toggle visibility of additional employee data columns in the UI.
- **Version Control & Data Safety**: Automatic schema validation, backups, and migrations for safe updates

## Project Structure

- `app.py`: Main Streamlit application code
- `data/`: Directory containing CSV files (created on first run if they don't exist)
  - `employees.csv`: Employee records. Must contain 'Standard ID' and 'Work Email Address'.
  - `workshops.csv`: Workshop series definitions
  - `events.csv`: Event instances
  - `cohorts.csv`: Employee cohorts
  - `participants.csv`: Detailed participation records
- `backups/`: Directory for automatic and manual backups (created on first run)
- `version.json`: Tracks schema version and migration status

## Requirements

- Python 3.9+
- Streamlit
- pandas

## Setup

1. **Clone the repository**:
   ```
   git clone https://github.com/rozenborg/records.git
   cd records
   ```

2. **Create a virtual environment**:
   ```
   python3.9 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```
   pip install -r requirements.txt
   ```

4. **Prepare your data files**:
   - Create a `data/` directory in the project root (the app will create it if it doesn't exist)
   - Place your `employees.csv` file in the `data/` directory
   - The `employees.csv` file **must** contain columns named `Standard ID` and `Work Email Address`
   - Other columns in your CSV will be loaded automatically
   - The application will create empty files for other required CSVs (`workshops.csv`, `events.csv`, `cohorts.csv`, `participants.csv`) on first run

5. **Run the application**:
   ```
   streamlit run app.py
   ```

## Usage

### Employees Section
- The application loads data from `data/employees.csv`.
- Ensure your CSV has `Standard ID` and `Work Email Address` columns.
- Other columns from your CSV will be loaded automatically.
- Use the sidebar options to toggle the display of these additional columns in the table.
- Tag employees with special categories (e.g., Program Lead) if a "Categories" column is present in your CSV and selected for display.
- View and edit employee data.

### Workshop Series Section
- Define workshop series with skills and learning goals.
- Track workshop instances and participation.

### Events Section
- Create and manage events of different types.
- Associate workshop events with their workshop series.
- Generate unique event IDs automatically.

### Cohorts Section
- Create cohorts of employees.
- Track nominations and actual participants.

### Participants Section
- Record employee participation in events.
- Upload lists of participants by ID or email.
- Mark employees as registered and/or participated.
- View and edit participation history.

## Data Safety & Version Control

### Schema Validation
- The app automatically validates CSV schemas against expected structure
- Missing columns are automatically added with default values
- Warning messages inform users about any automatic schema fixes

### Automatic Backups
- Backups are automatically created before applying migrations
- Use the "Data Management" sidebar to create manual backups
- Restore from previous backups if needed

### Schema Migrations
- The app tracks schema versions to ensure compatibility
- When updating to a new version, necessary data migrations are applied automatically
- Current schema version is displayed in the sidebar

## Data Storage

All data is stored as CSV files in the `data/` directory. The application will create empty CSV files with default headers if they are not found on startup, except for `employees.csv` which you must provide with at least 'Standard ID' and 'Work Email Address' columns.

## Performance Considerations

The application is optimized for:
- Efficient handling of large datasets (200,000+ employee records)
- Pagination for large tables
- Cached data loading to improve performance
- Memory-efficient CSV parsing

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 
# AI Adoption Program Tracker

A Streamlit-based application to manage and track participation in an AI Adoption Program. This tool helps program administrators organize workshops, events, cohorts, and track employee participation.

## Features

- **Employees Management**: Store and categorize employee information with special roles and departmental data
- **Workshop Series**: Define workshop series with skills and goals
- **Events Tracking**: Schedule and manage various event types (workshops, demos, meetings, conferences)
- **Cohorts Management**: Create and manage cohorts of employees
- **Participation Tracking**: Record and track employee registration and participation in events
- **Flexible CSV Import**: Map your own CSV column names to the required fields

## Project Structure

- `app.py`: Main Streamlit application code
- `data/`: Directory containing CSV files (created on first run)
  - `employees.csv`: Employee records
  - `workshops.csv`: Workshop series definitions
  - `events.csv`: Event instances
  - `cohorts.csv`: Employee cohorts
  - `participants.csv`: Detailed participation records
  - `csv_mappings.json`: Configuration for custom CSV column mappings

## Requirements

- Python 3.9+
- Streamlit
- pandas

## Setup

1. **Create a virtual environment**:
   ```
   python3.9 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```
   pip install -r requirements.txt
   ```

3. **Run the application**:
   ```
   streamlit run app.py
   ```

## Usage

### Employees Section
- Upload or manually enter employee details
- Configure CSV column mappings if your CSV has different column names
- Tag employees with special categories (Working Group Lead, Train-the-Trainer, etc.)
- View employee data organized by department

### Workshop Series Section
- Define workshop series with skills and learning goals
- Track workshop instances and participation

### Events Section
- Create and manage events of different types
- Associate workshop events with their workshop series
- Generate unique event IDs automatically

### Cohorts Section
- Create cohorts of employees
- Track nominations and actual participants

### Participants Section
- Record employee participation in events
- Upload lists of participants by ID or email
- Mark employees as registered and/or participated
- View and edit participation history

## Custom CSV Import

The application supports custom CSV formats for the employees table:

1. Prepare your CSV file with your own column names
2. Go to the Employees section
3. Use the "CSV Column Mapping" form to map your columns to the required fields:
   - Standard ID (required)
   - Email (required)
   - Location
   - Job Title
   - Department levels (L1-L6)
   - Manager levels (L1-L6)
   - Categories

The mappings are saved in `data/csv_mappings.json` and will be remembered between sessions.

## Data Storage

All data is stored as CSV files in the `data/` directory, making it easy to:
- Back up data with simple file copies
- Import/export data with spreadsheet applications
- Integrate with other systems via CSV import/export

## Performance Considerations

The application is optimized for:
- Efficient handling of large datasets (200,000+ employee records)
- Pagination for large tables
- Cached data loading to improve performance
- Memory-efficient CSV parsing

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 
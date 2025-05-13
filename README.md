# AI Adoption Program Tracker

A Streamlit-based application to manage and track participation in an AI Adoption Program. This tool helps program administrators organize workshops, events, cohorts, and track employee participation.

## Features

- **Employees Management**: Store and categorize employee information with special roles and departmental data
- **Workshop Series**: Define workshop series with skills and goals
- **Events Tracking**: Schedule and manage various event types (workshops, demos, meetings, conferences)
- **Cohorts Management**: Create and manage cohorts of employees
- **Participation Tracking**: Record and track employee registration and participation in events

## Project Structure

- `app.py`: Main Streamlit application code
- `data/`: Directory containing CSV files (created on first run)
  - `employees.csv`: Employee records
  - `workshops.csv`: Workshop series definitions
  - `events.csv`: Event instances
  - `cohorts.csv`: Employee cohorts
  - `participants.csv`: Detailed participation records

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
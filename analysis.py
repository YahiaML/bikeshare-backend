import time
import pandas as pd
import numpy as np

# ─────────────────────────────────────────────
# REQUIRED & OPTIONAL COLUMNS
# ─────────────────────────────────────────────

REQUIRED_COLUMNS = [
    'Start Time',
    'End Time',
    'Trip Duration',
    'Start Station',
    'End Station',
    'User Type'
]

OPTIONAL_COLUMNS = ['Gender', 'Birth Year']

# ─────────────────────────────────────────────
# VALIDATE COLUMNS
# ─────────────────────────────────────────────

def validate_columns(df):
    """
    Checks if the uploaded CSV has the required columns.
    Returns a dict with:
      - valid (bool)
      - missing_columns (list)
      - available_optional (list)
      - message (str)
    """
    uploaded_columns = [col.strip() for col in df.columns.tolist()]
    missing = [col for col in REQUIRED_COLUMNS if col not in uploaded_columns]
    available_optional = [col for col in OPTIONAL_COLUMNS if col in uploaded_columns]

    if missing:
        return {
            "valid": False,
            "missing_columns": missing,
            "available_optional": available_optional,
            "message": (
                f"Your file is missing the following required columns: {', '.join(missing)}. "
                f"Please make sure your CSV follows the standard bikeshare format and try again."
            )
        }

    return {
        "valid": True,
        "missing_columns": [],
        "available_optional": available_optional,
        "message": "File is valid and ready for analysis."
    }

# ─────────────────────────────────────────────
# GET AVAILABLE FILTERS FROM DATA
# ─────────────────────────────────────────────

def get_available_filters(df):
    """
    Dynamically extracts available months and days from the uploaded data.
    Returns a dict with available months and days for the UI dropdowns.
    """
    df = df.copy()
    df['Start Time'] = pd.to_datetime(df['Start Time'], errors='coerce')

    # ── Drop rows where Start Time could not be parsed ──
    df = df.dropna(subset=['Start Time'])

    month_map = {
        1: "January", 2: "February", 3: "March",
        4: "April",   5: "May",      6: "June",
        7: "July",    8: "August",   9: "September",
        10: "October",11: "November",12: "December"
    }

    available_month_nums = sorted(df['Start Time'].dt.month.dropna().unique().tolist())
    available_months = [month_map[int(m)] for m in available_month_nums if int(m) in month_map]

    available_days = sorted(df['Start Time'].dt.day_name().dropna().unique().tolist())

    return {
        "months": available_months,
        "days": available_days
    }

# ─────────────────────────────────────────────
# LOAD & FILTER DATA
# ─────────────────────────────────────────────

def load_data(df, month, day):
    """
    Filters the dataframe based on month and day selections.
    month: a month name string (e.g. 'January'), 'all', or None
    day:   a day name string (e.g. 'Monday'), 'all', or None
    """
    df = df.copy()
    df['Start Time'] = pd.to_datetime(df['Start Time'])
    df['month']      = df['Start Time'].dt.month_name().str.lower()
    df['day']        = df['Start Time'].dt.day_name()
    df['hour']       = df['Start Time'].dt.hour

    # Apply month filter
    if month and month.lower() != 'all':
        df = df[df['month'] == month.lower()]

    # Apply day filter
    if day and day.lower() != 'all':
        df = df[df['day'] == day.title()]

    return df

# ─────────────────────────────────────────────
# TIME STATS
# ─────────────────────────────────────────────

def time_stats(df, month, day):
    """
    Calculates the most frequent times of travel.
    Returns a dictionary of results.
    """
    start_time = time.time()
    result = {}

    # Most common month — only show if no specific month was selected
    if not month or month.lower() == 'all':
        result['most_common_month'] = df['month'].mode()[0].title()

    # Most common day — only show if no specific day was selected
    if not day or day.lower() == 'all':
        result['most_common_day'] = df['day'].mode()[0]

    # Most common hour — always shown
    result['most_common_hour'] = int(df['hour'].mode()[0])

    # Hour distribution for chart
    hour_counts = df['hour'].value_counts().sort_index()
    result['hour_distribution'] = [
        {"hour": int(h), "count": int(c)}
        for h, c in hour_counts.items()
    ]

    result['processing_time'] = round(time.time() - start_time, 4)
    return result

# ─────────────────────────────────────────────
# STATION STATS
# ─────────────────────────────────────────────

def station_stats(df):
    """
    Calculates the most popular stations and trips.
    Returns a dictionary of results.
    """
    start_time = time.time()
    result = {}

    result['most_common_start_station'] = df['Start Station'].mode()[0]
    result['most_common_end_station']   = df['End Station'].mode()[0]

    df['trip'] = df['Start Station'] + " → " + df['End Station']
    result['most_common_trip'] = df['trip'].mode()[0]

    # Top 5 start stations for chart
    top_starts = df['Start Station'].value_counts().head(5)
    result['top_start_stations'] = [
        {"station": s, "count": int(c)}
        for s, c in top_starts.items()
    ]

    # Top 5 end stations for chart
    top_ends = df['End Station'].value_counts().head(5)
    result['top_end_stations'] = [
        {"station": s, "count": int(c)}
        for s, c in top_ends.items()
    ]

    result['processing_time'] = round(time.time() - start_time, 4)
    return result

# ─────────────────────────────────────────────
# TRIP DURATION STATS
# ─────────────────────────────────────────────

def trip_duration_stats(df):
    """
    Calculates total and average trip duration.
    Returns a dictionary of results.
    """
    start_time = time.time()
    result = {}

    total_seconds   = int(df['Trip Duration'].sum())
    average_seconds = float(df['Trip Duration'].mean())

    # Convert to readable format
    def seconds_to_hms(seconds):
        seconds = int(seconds)
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return {"hours": h, "minutes": m, "seconds": s}

    result['total_duration']        = seconds_to_hms(total_seconds)
    result['average_duration']      = seconds_to_hms(average_seconds)
    result['total_duration_raw']    = total_seconds
    result['average_duration_raw']  = round(average_seconds, 2)

    result['processing_time'] = round(time.time() - start_time, 4)
    return result

# ─────────────────────────────────────────────
# USER STATS
# ─────────────────────────────────────────────

def user_stats(df, available_optional):
    """
    Calculates user statistics.
    available_optional: list of optional columns present in the uploaded file.
    Returns a dictionary of results.
    """
    start_time = time.time()
    result = {}

    # User type counts
    user_type_counts = df['User Type'].value_counts()
    result['user_types'] = [
        {"type": str(t), "count": int(c)}
        for t, c in user_type_counts.items()
    ]

    # Gender counts — only if column exists
    if 'Gender' in available_optional:
        gender_counts = df['Gender'].value_counts()
        result['gender'] = [
            {"gender": str(g), "count": int(c)}
            for g, c in gender_counts.items()
        ]
    else:
        result['gender'] = None

    # Birth year stats — only if column exists
    if 'Birth Year' in available_optional:
        result['birth_year'] = {
            "earliest":    int(df['Birth Year'].min()),
            "most_recent": int(df['Birth Year'].max()),
            "most_common": int(df['Birth Year'].mode()[0])
        }
    else:
        result['birth_year'] = None

    result['processing_time'] = round(time.time() - start_time, 4)
    return result

# ─────────────────────────────────────────────
# MAIN ANALYSIS RUNNER
# ─────────────────────────────────────────────

def run_analysis(file_path, month=None, day=None):
    """
    Master function that runs the full analysis pipeline.
    Called by the FastAPI endpoint.
    Returns a single structured dict with all results.
    """
    # Load raw CSV
    raw_df = pd.read_csv(file_path)

    # Validate columns
    validation = validate_columns(raw_df)
    if not validation['valid']:
        return {"error": True, "message": validation['message']}

    available_optional = validation['available_optional']

    # Filter data
    df = load_data(raw_df, month, day)

    if df.empty:
        return {
            "error": True,
            "message": "No data found for the selected filters. Please try a different combination."
        }

    # Run all analysis functions
    return {
        "error":          False,
        "time_stats":     time_stats(df, month, day),
        "station_stats":  station_stats(df),
        "duration_stats": trip_duration_stats(df),
        "user_stats":     user_stats(df, available_optional),
        "meta": {
            "total_rows":         len(df),
            "available_optional": available_optional,
            "filter_applied": {
                "month": month or "all",
                "day":   day   or "all"
            }
        }
    }
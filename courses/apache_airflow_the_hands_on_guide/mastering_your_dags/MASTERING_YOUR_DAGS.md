# Mastering Your DAGs

---

- [1. Definitions](#1-definitions)
- [2. Insights on Scheduling and Triggering](#2-insights-on-scheduling-and-triggering)
- [3. Best Practices](#3-best-practices)
- [4. Common Scheduling Presets](#4-common-scheduling-presets)
- [5. Backfill and Catchup](#5-backfill-and-catchup)
- [6. Dealing with Timezones in Airflow](#6-dealing-with-timezones-in-airflow)
- [7. How to make your tasks dependent](#7-how-to-make-your-tasks-dependent)
- [8. Visual Examples](#8-visual-examples)

---

## **1. Definitions**

### `start_date`

* **Description**: Represents the date from which tasks within your DAG can be scheduled and triggered.

### `schedule_interval`

* **Description**: Specifies the interval from the `start_date` at which your DAG should run.
* **Note**: The DAG will begin scheduling from the `start_date` and will run after every `schedule_interval`.

### `execution_date`

* **Description**: This is NOT the date when the DAG has run. It refers to the beginning of the period being processed,
  which is (`start_date` - `schedule_interval`).

---

## **2. Insights on Scheduling and Triggering**

* A DAG with a `start_date` at 10AM and a `schedule_interval` of 10 minutes will actually run at 10:10AM for data
  corresponding to 10AM.
* This is **AFTER** `start_date` + `schedule_interval`, indicating the **END** of the period.

---

## **3. Best Practices**

### **3.1 Tips for defining `start_date`**

1. Use a `datetime` object, e.g., `datetime.datetime(2023,9,16)`.
2. The date can be past or future.
3. While it's possible to set this at the task level, it's best to set the `start_date` globally at the DAG level (
   through `default_args`) and avoid dynamic values like `datetime.now()`.

### **3.2 Recommendations for `schedule_interval`**

1. Prefer Cron expressions (e.g., `0 * * * *`).
2. Alternatively, use Timedelta objects (e.g., `datetime.timedelta(days=1)`).
3. According to the official documentation, Cron expressions are preferred over timedelta objects.

### **3.3 A note on `end_date`**

* Represents the date your DAG or Task should stop scheduling.
* The default is `None`.
* Follow the same recommendations as for `start_date`.

---

## **4. Common Scheduling Presets**

|  Preset  | Meaning                                         | Cron Expression |
|:--------:|-------------------------------------------------|:---------------:|
|   None   | No schedule. Triggered manually.                |        -        |
|  @once   | Run only once.                                  |        -        |
| @hourly  | Runs at the start of every hour.                |    0 * * * *    |
|  @daily  | Runs at midnight daily.                         |    0 0 * * *    |
| @weekly  | Runs at midnight on Sundays.                    |    0 0 * * 0    |
| @monthly | Runs at midnight on the first day of the month. |    0 0 1 * *    |
| @yearly  | Runs at midnight on January 1.                  |    0 0 1 1 *    |

---

## **5. Backfill and Catchup**

### **5.1 DagRun**

* Created by the scheduler.
* Represents an instance of a DAG at a specific time.
* Contains tasks to run.
* Characteristics: **Atomic, Idempotent**.

### **5.2 How to Control Catchup?**

1. Set the `catchup` parameter in the DAG definition to `True` or `False`.
2. Modify the `catchup_by_default` setting in `airflow.cfg`.
3. By default, `catchup_by_default=True`.

---

## **6. Dealing with Timezones in Airflow**

### **6.1 Best Practices**

1. **ALWAYS** use timezone-aware datetime objects.
2. `datetime.datetime()` in Python yields naive datetime objects by default.
3. Datetime without timezones are **not** in UTC.
4. Use `airflow.timezone` to create timezone-aware datetime objects.
5. Airflow can also handle timezone conversion for you.

### **6.2 Airflow and Timezones**

* Supports timezones.
* Stores datetime info in UTC.
* Displays datetime in UTC in the user interface.
* Developers are responsible for handling datetimes.
* Default timezone in `airflow.cfg` is UTC: `default_timezone = utc`.
* Airflow uses the `pendulum` Python library for time zones.

### **6.3 Make Your DAG Timezone-Aware**

```python
import pendulum
from airflow import DAG
from datetime import datetime

local_tz = pendulum.timezone("Europe/Paris")

default_args = {
    'start_date': datetime(2023, 9, 18, tzinfo=local_tz),
    'owner': 'airflow'
}

with DAG(dag_id='my_dag', default_args=default_args) as dag:
    ...
 ```

### **6.4 Cron Schedules**

* Airflow assumes you will always want to run at the exact same time and will ignore the DST (Daylight Saving Time). For
  example, `schedule_interval= 0 5 * * *` will always trigger your DAG at `5 PM GMT +1` every day **regardless if DST is
  in effect**.

### **6.5 Timedelta**

* When the `schedule_interval` is set with a `time_delta`, Airflow assumes you always want to run with the specified
  interval. Examples: `timedelta(hours=2)` will always trigger your DAG 2 hours later.

### **6.6 Cron vs Timedelta with Catchup**

|             Date             | 30 March 2023 2 AM (UTC+1) | 31 March 2023 1:59 AM (UTC+1) | 31 March 2023 3 AM (DST - UTC +2) | 1 April 2023 2 AM (UTC+2) | 1 April 2023 3 AM (UTC+2) |
|:----------------------------:|:--------------------------:|:-----------------------------:|:---------------------------------:|:-------------------------:|:-------------------------:|
|   Cron with Catchup=False    |            RUN             |             WAIT              |              NO RUN               |            RUN            |          NO RUN           |
|    Cron with Catchup=True    |            RUN             |             WAIT              |                RUN                |            RUN            |          NO RUN           |
| Timedelta with Catchup=False |            RUN             |             WAIT              |                RUN                |          NO RUN           |            RUN            |
| Timedelta with Catchup=True  |            RUN             |             WAIT              |                RUN                |          NO RUN           |            RUN            |

* **Note**: For cron expressions, when DST happens 2AM => 3AM, the scheduler thinks the DAGRun of the 31 of March has
  been skipped and since Catchup=False, it won't get executed. The next DagRun will be the 1st of April at 2AM.

## **7. How to make your tasks dependent**

### **7.1 depends_on_past**

* Defined at **task level**.
* If the **previous task instance** failed, the current task is not executed.
* Consequently, the current task has **no status**.
* First task instance with `start_date` allowed to run.

### **7.2 wait_for_downstream**

* Defined at **task level**.
* An instance of task X will wait for **tasks** `immediately` **downstream** of the **previous instance** of task X to
  finish **successfully** before it runs.

## **8. Visual Examples**

### **8.1 Example for `depends_on_past`**

Consider a DAG with tasks A >> B (depends_on_past=True) >> C.

1. **Run 1**: All tasks succeed (A >> B >> C).
2. **Run 2**: A succeeds, B fails, C is skipped.
3. **Run 3**: A succeeds but B waits because it depends on its past instance (which had failed in Run 2).

### **8.2 Example for `wait_for_downstream`**

For A task with `wait_for_downstream=True` and a DAG with tasks A (wait_for_downstream=True) >> B >> C.

1. **Run 1**: All tasks succeed (A >> B >> C).
2. **Run 2**: A succeeds, B fails, C is skipped.
3. **Run 3**: None of the tasks start because Task A from Run 2 is waiting for B and C from Run 2 to complete
   successfully.

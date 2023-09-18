# `start_date` and `schedule_interval` parameters demystified

---

## Key Definitions

### `start_date`

- **Definition**: Represents the date from which tasks within your DAG can be scheduled and triggered.

### `schedule_interval`

- **Definition**: Dictates the interval from the `start_date` at which your DAG should be triggered.

- **Note**: The DAG will begin scheduling from the `start_date` and will be triggered after every `schedule_interval`.

## Understanding `execution_date`

- The `execution_date` is NOT the date when the DAG has been run.
- It corresponds to the **beginning of the processed period** (`start_date` - `schedule_interval`).

## Insights on Scheduling and Triggering

- **Clarification**:
    - A DAG with a `start_date` at 10AM and a `schedule_interval` of 10 minutes will actually be executed at 10:10AM for
      data corresponding to 10AM.
    - This is **AFTER** `start_date` + `schedule_interval`, marking the **END** of the period.

## Tips for defining `start_date`

- Use a `datetime` object, e.g., `datetime.datetime(2023,9,16)`.
- The date can be either in the past or the future.
- It's possible to set this at the task level.
- **Best Practice**: Set the `start_date` globally at the DAG level (through `default_args`) and avoid dynamic values
  like `datetime.now()`.

## Recommendations for `schedule_interval`

- Utilize Cron expressions (e.g., `0 * * * *`).
- Alternatively, use Timedelta objects (e.g., `datetime.timedelta(days=1)`).
- **Best Practice**: Favor `cron` expressions over `timedelta` objects, as advised in official documentation.

## A note on `end_date`

- This signifies the date at which your DAG or Task should cease scheduling.
- Default setting is `None`.
- Adhere to the same recommendations as `start_date`.

## Common Scheduling Presets

| Preset   | Meaning                                                        | Cron Expression |
|----------|----------------------------------------------------------------|-----------------|
| None     | No schedule. Triggered manually.                               | -               |
| @once    | Scheduled for one-time execution only.                         | -               |
| @hourly  | Executes at the start of every hour.                           | 0 * * * *       |
| @daily   | Executes at midnight daily.                                    | 0 0 * * *       |
| @weekly  | Executes at midnight on Sundays.                               | 0 0 * * 0       |
| @monthly | Executes at midnight on the first day of the month.            | 0 0 1 * *       |
| @yearly  | Executes at midnight on the first day of the year (January 1). | 0 0 1 1 *       |

# Backfill and Catchup

___

## DagRun

* The scheduler creates a `DagRun` object
* Describes and instance of a given `DAG` in time
* Contains tasks to execute.
* **Atomic, Idempotent**

## How to turn on/off Catchup?

Either By:

* Setting the parameter `catchup` in the DAG definition to `True` of `False`
* Changing the parameter `catchup_by_default` in `airflow.cfg`
* By default, `catchup_by_default=True`
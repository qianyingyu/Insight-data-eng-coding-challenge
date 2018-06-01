# Insight-data-eng-coding-challenge
- by Qianying Yu


## Solution:

The basic idea to solve this challenge is to aggregate input data as time series stream data and use a real time dictionary to store the information that has not expired beyond the inactive period.
 
## `sessionization.py`:

 * **stream**: dictionary to store aggregated stream data, **date&time** as key of stream dictionary and **ip** + **other information** as value.
 * **rt_record**: dictionary to store real time record that has not expired beyond the inactive period and has not been outputed.
 
## `your-own-test_1/log.csv`:
 * use the original csv file `log20170630.csv` provided by SEC.

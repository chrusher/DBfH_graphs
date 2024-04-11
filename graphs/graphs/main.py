import gevent.monkey
gevent.monkey.patch_all()

import datetime
import itertools
import json
import logging
import operator
import os

import argh
import bokeh
import bokeh.plotting
import bokeh.models
import bokeh.palettes
import bokeh.settings
import numpy as np
import psycopg2
import requests

import common
from common import database

def format_year(year):
    if year > 10:
        year += 2006
    return 'DBfH {}'.format(year)     

def parse_json(json_donations, start_date, end_hour=np.inf, every_five=True):

    end_hour = float(end_hour)
    times = []
    donations = []
    for entry in json_donations:
        times.append(datetime.datetime(*entry[:5]).isoformat())
        donations.append(entry[5])

    times = np.array(times, dtype=np.datetime64)
    donations = np.asarray(donations)

    start_time = np.datetime64(start_date)
    bustimes = np.array(times - start_time, dtype=np.int_)
    
    trimmed_bustimes = bustimes[(bustimes <= 60 * 60 * end_hour) & (bustimes >= 0)]
    trimmed_donations = donations[(bustimes <= 60 * 60 * end_hour) & (bustimes >= 0)]

    if every_five:
        five_bustimes = trimmed_bustimes[::5]
        five_donations = trimmed_donations[::5]
        return five_bustimes, five_donations
    else:
        return trimmed_bustimes, trimmed_donations
       
def load_previous_donations(start_end_times, donation_url_template, timeout):
        
        all_years = {}
        for year in start_end_times:
            start, end = start_end_times[year]
            if not end:
                current_year = year
                continue                
            
            url = donation_url_template.format(year, year)
            logging.info('Loading {}'.format(url))
            year_json = requests.get(url, timeout=timeout).json()
            all_years[year] = parse_json(year_json, start, end, year >= 5)
            
        return all_years, current_year
    
def all_years_donations_graph(start_end_times, all_years, current_year, current_json, base_dir):
    
    logging.info('Generating all years donation graph')
    p = bokeh.plotting.figure(x_axis_label='Bus Time', y_axis_label='Donations', x_range=(0, 60 * 60 * 172),
                              width=1280, height=720, active_scroll='wheel_zoom',
                              tools='pan,wheel_zoom,box_zoom,reset')

    p.add_tools(bokeh.models.HoverTool(tooltips=[('', '$name'), ('Bustime', '@Bustime{00:00:00}'),
                                                 ('Donations', '$@Donations{0,0.00}')]))
    for year in start_end_times:
        label = format_year(year)        
        if year != current_year:
            times, donations = all_years[year]
            line_width = 2
        else:
            times, donations = parse_json(current_json, start_end_times[year][0], every_five=False)
            line_width = 3
        model = bokeh.models.ColumnDataSource(data={'Bustime':times, 'Donations':donations})
        p.line(x='Bustime', y='Donations', source=model, line_width=line_width,
               line_color=bokeh.palettes.Category20[20][current_year - year],
               legend_label=label, name=label)


    p.xaxis.ticker = bokeh.models.AdaptiveTicker(mantissas=[60, 120, 300, 600, 1200, 3600, 7200, 10800, 43200, 86400], base=10000000)
    p.xaxis.formatter = bokeh.models.NumeralTickFormatter(format='00:00:00')
    p.yaxis.formatter = bokeh.models.NumeralTickFormatter(format='$0,0')

    p.legend.location = "top_left"
    p.legend.click_policy="hide"

    output_path = os.path.join(base_dir, 'all_years_donations.html')
    bokeh.plotting.output_file(filename=output_path, title='DBfH All Years Donations')
    bokeh.plotting.save(p, filename=output_path)
    logging.info('{} Saved'.format(output_path))
    
def shifts_graph(start_end_times, current_year, current_json, base_dir, shifts):
    
    logging.info('Generating DBfH {} shifts graph'.format(current_year))
    times, donations = parse_json(current_json, start_end_times[current_year][0], every_five=False)
    start_hour = int(start_end_times[current_year][0][11:13])

    hours = times / 3600 + start_hour
    mod_hours = hours % 24
    n_days = int(hours.max() / 24) + 1
    logging.info(str(hours.max()))
    
    p = bokeh.plotting.figure(x_axis_label='Hour of Day', y_axis_label='Donations', x_range=(0, 24 * 3600),
                          width=1280, height=720, active_scroll='wheel_zoom',
                          tools='pan,wheel_zoom,box_zoom,reset')
    p.add_tools(bokeh.models.HoverTool(tooltips=[('', '$name'), ('Hour of Day', '@Hours{00:00:00}'),
                                       ('Donations', '$@Donations{0,0.00}')]))
    
    for day in range(n_days):

        for shift in shifts:
            in_range = (hours >= day * 24 + shift[1]) & (hours <= day * 24 + shift[2])
            hours_in_range = mod_hours[in_range]
            if mod_hours[in_range].size:
                
                if hours_in_range[-1] == 0.:
                    hours_in_range[-1] = 24  
                model = bokeh.models.ColumnDataSource(data={'Hours':hours_in_range * 3600, 'Donations':donations[in_range] - donations[in_range][0]})
                p.line(x='Hours', y='Donations', source=model, line_color=bokeh.palettes.Category10[10][day],
                       line_width=2, legend_label='Day {}'.format(day + 1), name='Day {} {}'.format(day + 1, shift[0]))
                
    p.xaxis.ticker = bokeh.models.AdaptiveTicker(mantissas=[60, 120, 300, 600, 1200, 3600, 7200, 10800, 43200, 86400], base=10000000)
    p.xaxis.formatter = bokeh.models.NumeralTickFormatter(format='00:00:00')
    p.yaxis.formatter = bokeh.models.NumeralTickFormatter(format='$0,0')

    p.legend.location = "top_left"
    p.legend.click_policy="hide"                
    
    output_path = os.path.join(base_dir, 'DBfH_{}_shifts_graph.html'.format(current_year))
    bokeh.plotting.output_file(filename=output_path, title='{} Shift Donations'.format(format_year(current_year)))
    bokeh.plotting.save(p, filename=output_path)
    logging.info('{} Saved'.format(output_path))    


def odometer_graph(db_manager, start_end_times, current_year, base_dir):

    start = start_end_times[current_year][0]
    connection = db_manager.get_conn()
    
    logging.info('Postprocessing DBfH {} odometer data'.format(current_year))
    results = database.query(connection, """
            SELECT timestamp, odometer
            FROM bus_data
            WHERE odometer > 0
            AND timestamp > %(start)s
            --AND NOT segment LIKE '%%partial%%'
            ORDER BY timestamp;
            """, start=start)
    rows = results.fetchall()

    times, miles = zip(*rows)
    times = np.asarray([np.datetime64(time) for time in times])
    miles = np.asarray(miles)

    seconds = (times - times[0]) / np.timedelta64(1, 's')
    max_speed = 45 / 3600

    good = []
    suspect = []
    for i in range(1, len(times) - 1):

        previous_diff = miles[i] - miles[i - 1]
        if previous_diff < 0 or previous_diff > max_speed * (seconds[i] - seconds[i - 1]):
            suspect.append(i)
            continue
        next_diff = miles[i + 1] - miles[i]
        if next_diff < 0 or next_diff > max_speed * (seconds[i + 1] - seconds[i]):
            suspect.append(i)
            continue
        # handle big jumps to apparently good data
        if good and miles[i] - miles[good[-1]] > max_speed * (seconds[i] - seconds[good[-1]]):
            suspect.append(i)
            continue
        # try to filter out bad data at the start
        if not good and miles[i] > 1000:
            suspect.append(i)
            continue

        good.append(i)

    corrected_miles = np.zeros(miles.size)
    corrected_miles[good] = miles[good]

    for k, g in itertools.groupby(enumerate(suspect), lambda x:x[0]-x[1]):
        group = map(operator.itemgetter(1), g)
        group = list(map(int, group))

        to_fix = []
        for i in group:
            back = 1

            while True:
                if corrected_miles[i - back]:
                    diff = miles[i] - corrected_miles[i - back]
                    max_diff = max_speed * (seconds[i] - seconds[i - back])
                    if diff >= 0 and diff <= max_diff and miles[i] <= miles[group[-1] + 1]:
                        corrected_miles[i] = miles[i]
                    break
                else:
                    back += 1

            if not corrected_miles[i]:
                to_fix.append(i)

        for k, g in itertools.groupby(enumerate(to_fix), lambda x:x[0]-x[1]):
            subgroup = map(operator.itemgetter(1), g)
            subgroup = list(map(int, subgroup))

            # ignore data from before the first good measurement or after crashes
            if subgroup[0] < good[0] or corrected_miles[subgroup[0] - 1] > corrected_miles[subgroup[-1] + 1]:
                continue

            m = (corrected_miles[subgroup[-1] + 1] - corrected_miles[subgroup[0] - 1]) / (seconds[subgroup[-1] + 1] - seconds[subgroup[0] - 1])
            b = corrected_miles[subgroup[-1] + 1] - m * seconds[subgroup[-1] + 1]       

            for i in subgroup:
                corrected_miles[i] = m * seconds[i] + b

    # custom handling of the start and end
    if 0 <= corrected_miles[1] - miles[0] < max_speed * (seconds[1] - seconds[0]):
        corrected_miles[0] = miles[0]

    if 0 <= miles[-1] - corrected_miles[-2] < max_speed * (seconds[-1] - seconds[-2]):
        corrected_miles[-1] = miles[-1]

    minutes = []
    mean_miles = []
    minute_map = {}

    minute = np.datetime64(start)
    while minute < times[-1]:

        minutes.append(minute)
        in_range = (times >= minute) & (times < minute + np.timedelta64(1, 'm')) & (corrected_miles > 0)
        if in_range.any():
            mean_miles.append(corrected_miles[in_range].mean())
        else:
            mean_miles.append(0)

        minute_map[minute] = mean_miles[-1]
        minute += np.timedelta64(1, 'm')
    
    odometer_json = [[str(minute).split('.')[0], mile] for minute, mile in zip(minutes, mean_miles)]
    output_path = os.path.join(base_dir, 'DBfH_{}_odometer.json'.format(current_year))
    json.dump(odometer_json, open(output_path, 'w'))
    
    logging.info('Generating DBfH {} odometer graph'.format(current_year))
    
    mean_miles = np.asarray(mean_miles)
    mean_miles[mean_miles == 0] = np.nan
    
    results = database.query(connection, """
        SELECT event_start, category, description
        FROM events
        WHERE (category = 'Game Event' OR category = 'Crash')
        AND event_start > %(start)s
        ORDER BY event_start;
        """, start=start)
    events = results.fetchall()    
    
    p = bokeh.plotting.figure(x_axis_label='Bus Time', y_axis_label='Miles', x_range=(0, 60 * 60 * 172),
                              width=1280, height=720, active_scroll='wheel_zoom',
                              tools='pan,wheel_zoom,box_zoom,reset')
    p.add_tools(bokeh.models.HoverTool(tooltips=[('', '$name'), ('Bustime', '@Bustime{00:00:00}'),
                                                 ('Miles', '@Miles{0.00}')]))

    minute_seconds = (np.asarray(minutes) - np.datetime64(start)) / np.timedelta64(1, 's')

    model = bokeh.models.ColumnDataSource(data={'Bustime':minute_seconds, 'Miles':mean_miles})

    p.line(x='Bustime', y='Miles', source=model, line_width=2,
           line_color='gray', legend_label='Odometer', name='Odometer')

    crash_times = []
    crash_miles = []
    bus_stop_times = []
    bus_stop_miles = []
    bug_splat_times = []
    bug_splat_miles = []
    point_times = []
    point_miles = []

    for row in events:
        event_time, catagory, description = row
        event_time = np.datetime64(event_time)
        mile = minute_map[event_time]
        second = (np.datetime64(event_time) - np.datetime64(start)) / np.timedelta64(1, 's')
        description = description.lower()
        if catagory == 'Crash':
            crash_times.append(second)
            crash_miles.append(mile)
        elif 'bus stop' in description:
            bus_stop_times.append(second)
            bus_stop_miles.append(mile)
        elif 'splat' in description:
            bug_splat_times.append(second)
            bug_splat_miles.append(mile)
        elif 'point' in description:
            point_times.append(second)
            point_miles.append(mile)           


    model = bokeh.models.ColumnDataSource(data={'Bustime':point_times, 'Miles':point_miles})
    p.square(x='Bustime', y='Miles', source=model, size=10, color='blue', legend_label='Point', name='Point')

    model = bokeh.models.ColumnDataSource(data={'Bustime':bus_stop_times, 'Miles':bus_stop_miles})
    p.circle(x='Bustime', y='Miles', source=model, size=10, color='red', legend_label='Bus Stop', name='Bus Stop')

    model = bokeh.models.ColumnDataSource(data={'Bustime':crash_times, 'Miles':crash_miles})
    p.x(x='Bustime', y='Miles', source=model, size=10, line_width=3, color='black', legend_label='Crash', name='Crash')

    model = bokeh.models.ColumnDataSource(data={'Bustime':bug_splat_times, 'Miles':bug_splat_miles})
    p.asterisk(x='Bustime', y='Miles', source=model, size=10, line_width=2, color='green', legend_label='Bug Splat', name='Bug Splat')

    p.xaxis.ticker = bokeh.models.AdaptiveTicker(mantissas=[60, 120, 300, 600, 1200, 3600, 7200, 10800, 43200, 86400], base=10000000)
    p.xaxis.formatter = bokeh.models.NumeralTickFormatter(format='00:00:00')

    p.legend.location = "top_left"
    p.legend.click_policy="hide"
 
    output_path = os.path.join(base_dir, 'DBfH_{}_odometer_graph.html'.format(current_year))
    bokeh.plotting.output_file(filename=output_path, title='{} Odometer and Game Events'.format(format_year(current_year)))
    bokeh.plotting.save(p, filename=output_path)
    logging.info('{} Saved'.format(output_path))  
    

@argh.arg('--base-dir', help='Directory where graphs are output. Default is current working directory.')
def main(donation_url_template, connection_string, base_dir='.'):
    
    stopping = gevent.event.Event()  
    
    logging.getLogger('bokeh').setLevel(logging.WARNING)
    logging.info('Using Bokeh {}'.format(bokeh.__version__)) 
    
    db_manager = database.DBManager(dsn=connection_string)
    
    delay = 60 * 1
    timeout = 15
    
    shifts = [['Zeta Shift',   0, 6],
              ['Dawn Guard', 6, 12],
              ['Alpha Flight',  12, 18],
              ['Night Watch', 18, 24]]
    
    # First load data required 
    logging.info('Loading start and end times')
    start_end_path = os.path.join(base_dir, 'start_end_times.json')
    start_end_times = json.load(open(start_end_path))
    start_end_times = {int(year):start_end_times[year] for year in start_end_times}
    
    all_years, current_year = load_previous_donations(start_end_times, donation_url_template, timeout)
    current_url = donation_url_template.format(current_year, current_year)

    while not stopping.is_set():

        try:

            logging.info('Loading {}'.format(current_url))
            current_json = requests.get(current_url, timeout=timeout).json()
            
            all_years_donations_graph(start_end_times, all_years, current_year, current_json, base_dir)
            
            shifts_graph(start_end_times, current_year, current_json, base_dir, shifts)
            
            odometer_graph(db_manager, start_end_times, current_year, base_dir)


        except Exception:
            logging.exception('Plotting failed. Retrying')

        stopping.wait(delay)


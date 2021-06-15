from datetime import datetime
import os
from stream.teststream import TestStream
from evalunit.program import Program


def create_stream(f_name):
    stream = dict()
    event_counter = dict()
    measure_counter = dict()
    timestamp = 0
    atom_counter = 0
    sectors = set()
    with open(f_name, "r") as file:
        for line in file.readlines():
            the_split = line.split(":[")
            atoms_list = list(the_split[1].split("]")[0].split('"'))[1::2]
            """for i in range(0, len(atoms_list)):
                atom = atoms_list[i]
                pred_split = atom.split("(")
                params = pred_split[1].split(")")[0].split(",")
                if not pred_split[0] == "weather":
                    sectors.add(int(params[len(params) - 1]))
                event_counter[pred_split[0]] = event_counter.get(pred_split[0], 0) + 1
                measure_counter[pred_split[0]] = measure_counter.get(pred_split[0], dict())
                if params[1] == "":
                    measure_val = 0
                else:
                    measure_val = int(float(params[1]))
                attrb_vals = measure_counter[pred_split[0]].get(params[0], [measure_val, measure_val, 0, 0])
                if measure_val > attrb_vals[1]:
                    attrb_vals[1] = measure_val
                if measure_val < attrb_vals[0]:
                    attrb_vals[0] = measure_val
                attrb_vals[2] += 1  # len
                attrb_vals[3] += measure_val  # sum

                measure_counter[pred_split[0]][params[0]] = attrb_vals
                atom_counter += 1"""
            stream[int(the_split[0])] = atoms_list
            timestamp += 1

    print("Total number of atoms = {}".format(atom_counter))
    for atom in event_counter.items():
        print("Avg num of " + atom[0].upper() + " atoms/timestamp in stream = " + str(float(atom[1]) / timestamp))
    for items in measure_counter.items():
        print("Event: {}".format(items[0].upper()))
        for measure in items[1].items():
            print("Measure: {} --> min_value: {}; max_value: {}; avg: {}".format(measure[0].upper(), measure[1][0],
                                                                                 measure[1][1],
                                                                                 round(measure[1][3] / measure[1][2],
                                                                                       2)))
            atom_counter -= measure[1][2]
    print("Sectors list: {}\n".format(str(sectors)))
    assert atom_counter == 0
    return stream



def query_five(filename, output_stream):
    stream = create_stream(filename)

    rules = [
        "city(3.0):-",
	"city(4.0):-",
	"city(6.0):-",
	"town(8.0):-",
	"town(10.0):-",
	"town(1.0):-",
	"town(2.0):-",
	"town(5.0):-",
	"town(7.0):-",
	"town(9.0):-",
	
	"suburb(1.0, 3.0):-",
	"suburb(2.0, 3.0):-",
	"suburb(8.0, 4.0):-",
	"suburb(9.0, 4.0):-",
	"suburb(10.0, 3.0):-",
	"suburb(7.0, 6.0):-",
	"suburb(5.0, 6.0):-",
	
	"close(A, C, B) :- suburb(A, B) and suburb(C, B) and COMP(!=, A, C)",
	
	"@(T, highPollutionCo(SENS, SEC)) :- time_win(4, 0, 1, @(T, pollution(TYPE, MES, SENS, SEC)))"
	" and COMP(==, TYPE, 2.0) and COMP(>, MES, 125)",
	"@(T, highPollutionPM(SENS, SEC)) :- time_win(4, 0, 1, @(T, pollution(TYPE, MES, SENS, SEC)))"
	" and COMP(==, TYPE, 1.0) and COMP(>, MES, 125)",


	"highPollutionCo_cont(SENS, SEC) :- time_win(4, 0, 1, box(highPollutionCo(SENS, SEC)))",
	"highPollutionPM_cont(SENS, SEC) :- time_win(4, 0, 1, box(highPollutionPM(SENS, SEC)))",

	"industrialSens(SENS, SEC) :- highPollutionCo_cont(SENS, SEC) and highPollutionPM_cont(SENS, SEC)",
			
	"industrialSec(SEC) :- industrialSens(SENS1, SEC) and industrialSens(SENS2, SEC)"
	"and industrialSens(SENS3, SEC) and industrialSens(SENS4, SEC)"
	" and COMP(!=, SENS1, SENS2) and COMP(!=, SENS1, SENS3) and COMP(!=, SENS1, SENS4)"
	"and COMP(!=, SENS2, SENS3) and COMP(!=, SENS2, SENS4) and COMP(!=, SENS3, SENS4)",

	"anomaly(CITY) :- industrialSec(SEC1) and industrialSec(SEC2) and close(SEC1, SEC2, CITY)",

	"industrialArea(SEC) :- industrialSec(SEC) and city(SEC)",

	"alert(SEC) :- industrialArea(SEC) and anomaly(SEC)"
    ]

    ti, tf = min(stream.keys()), max(stream.keys())
    s = TestStream(stream, ti, tf)
    prog = Program(rules, s)
    total_time = 0
    positive_t = 0
    for t in range(0, tf):
        init_t = datetime.now()
        res, tuples = prog.evaluate(t)
        # print(str(tuples))
        tups = set()
        final_t = datetime.now()
        if t > ti:
            total_time += (final_t - init_t).total_seconds()
            print("Timestamp: {};  Seconds to evaluate: {};".format(t, (final_t - init_t).total_seconds()))

            if len(tuples) > 0 and res:
                for tup in tuples.items():
                    tups = filter(lambda x: "alert" in x, tup[1])
                    output_stream.write(str(tup[0]) + ": " + str(tup[1]) + "\n")
                    if len(tups) > 0:
                        positive_t += 1

            if t % 10 == 0:
                print("Average time per timestamp (secs): {};    Expected remained time (mins): {}".format(
                    total_time / (t - ti), ((total_time / (t - ti)) * (tf - t)) / 60))
    return tf - ti

laser_folder = os.getcwd() + "/"
input_folder = laser_folder + "laser_input_q5/"
output_folder = laser_folder + "q5_output/"
counter = 9
window_num = 0
query_dict = dict()
init_time = datetime.now()
for filename in os.listdir(input_folder):
    if counter == 0:
        break

    filename = "laser{}.txt".format(counter)
    print(filename)
    with open(output_folder + "output_" + filename, "w") as out:
        window_num += query_five(input_folder + filename, out)

    counter -= 1
end_time = datetime.now()
print(window_num)
print("\nnumber of window tested: {}\nTotal time in exec: {}\n".format(window_num,
                                                                       (end_time - init_time).total_seconds()))

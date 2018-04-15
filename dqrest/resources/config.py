from flask import make_response, render_template, request
from flask_restful import Resource
from dqrest.common.db import get_db
from dqrest.common.error import gen_error_response
from dqrest.common.hdfs import cat_file, save_to_hdfs, HDFSCONFIGDIR
import uuid


class Config(Resource):

    def get(self, file_id):
        file_path = "%s/%s.txt" % (HDFSCONFIGDIR, file_id)
        try:
            assessment_file = cat_file(file_path)
            if 'Content-Type' in request.headers and request.headers['Content-Type'] == 'text/plain':
                return assessment_file
            else:
                config_file = {}
                config_file['data'] = assessment_file
                config_file['id'] = file_id
                cur = get_db().cursor()
                rv = cur.execute(
                    'SELECT uuid FROM requests_assessment WHERE file_id = ?', (str(file_id),)).fetchall()
                connected_assessment_list = []
                for a in rv:
                    connected_assessment_list.append(a[0])
                config_file['used_by'] = connected_assessment_list
                headers = {'Content-Type': 'text/html'}
                return make_response(render_template('config_file.html', config_file=config_file), 200, headers)
        except Exception as e:
            print(e)
            return gen_error_response(404, "No such file")


def verify_config_json(json):
    return_value = True
    description = []
    if ("config_file_uuid" in json):
        return True, "Success, the config file is already here"
    if ("input" not in json):
        return_value = False
        description.append("input")
    if ("output" not in json):
        return_value = False
        description.append("output")
    if ("sel_attributes" not in json):
        return_value = False
        description.append("sel_attributes")
    else:
        sel_attributes_list = json["sel_attributes"]
        for s in range(len(sel_attributes_list)):
            if ("name" not in sel_attributes_list[s]):
                return_value = False
                description.append(
                    "name[in sel_attributes field] item number " + str(s + 1))
            if ("intervals" not in sel_attributes_list[s]):
                return_value = False
                description.append(
                    "intervals[in sel_attributes field] item number " + str(s + 1))
            if ("selected" not in sel_attributes_list[s]):
                return_value = False
                description.append(
                    "selected[in sel_attributes field] item number " + str(s + 1))
    if ("g_attributes" not in json):
        return_value = False
        description.append("g_attributes")
    if ("dimensions" not in json):
        return_value = False
        description.append("dimensions")
    else:
        dimensions_list = json["dimensions"]
        for d in range(len(dimensions_list)):
            if ("name" not in dimensions_list[d]):
                return_value = False
                description.append(
                    "name[in dimensions field] item number " + str(d + 1))
            elif(dimensions_list[d]["name"].lower() == "timeliness"):
                if ("volatility" not in dimensions_list[d]):
                    return_value = False
                    description.append(
                        "volatility[in dimensions field] item number " + str(d + 1))
            if ("granularity" not in dimensions_list[d]):
                return_value = False
                description.append(
                    "granularity[in dimensions field] item number " + str(d + 1))
    if ("mean_acc" not in json):
        return_value = False
        description.append("mean_acc")
    if ("interval_acc" not in json):
        return_value = False
        description.append("interval_acc")
    if ("add_con_rules" not in json):
        return_value = False
        description.append("add_con_rules")
    else:
        add_con_rules_list = json["add_con_rules"]
        for r in range(len(add_con_rules_list)):
            if("antecedent" not in add_con_rules_list[r]):
                return_value = False
                description.append(
                    "antecedent[in add_con_rules field] item number " + str(r + 1))
            if("consequent" not in add_con_rules_list[r]):
                return_value = False
                description.append(
                    "consequent[in add_con_rules field] item number " + str(r + 1))

    if ("source_summary" not in json):
        return_value = False
        description.append("source_summary")
    if ("con_rules" not in json):
        return_value = False
        description.append("con_rules")

    if return_value:
        return return_value, "Success"
    else:
        return return_value, "These fields must be in json file: " + " - ".join(description) + ".\nCheck the documentation"


def make_config_file(request):
    json = request.get_json()
    config_file_dict = {}
    if("config_file_uuid" in json):
        return json["config_file_uuid"], False
        # generating 1,2,9,10 and 13 lines of the config file
    file_id = str(uuid.uuid4())
    config_file_dict["input"] = json["input"]
    config_file_dict["output"] = json["output"]
    config_file_dict["mean_acc"] = json["mean_acc"]
    config_file_dict["interval_acc"] = json["interval_acc"]
    config_file_dict["source_summary"] = json["source_summary"]

    # generating 3,4,5 and 6 lines of the config file
    name_list = []
    interval_list = []
    selected_list = []
    for elem in json["sel_attributes"]:
        name_list.append(elem["name"])
        interval_list.append(",".join(elem["intervals"]))
        selected_list.append(",".join(elem["selected"]))
    config_file_dict["sel_attributes"] = ";".join(name_list)
    config_file_dict["values_intervals"] = ";".join(interval_list)
    config_file_dict["selected_values"] = ";".join(selected_list)
    grouping_list = [",".join(x) for x in json["g_attributes"]]
    config_file_dict["g_attributes"] = ";".join(grouping_list)

    # generating 7,8 and 12 lines of the config file
    name_list = []
    granularity_list = []
    config_file_dict["volatility"] = ""
    for elem in json["dimensions"]:
        name_list.append(elem["name"])
        granularity_list.append(",".join(elem["granularity"]))
        if (elem["name"].lower() == "timeliness"):
            try:
                config_file_dict["volatility"] = ";".join(elem["volatility"])
            except:
                config_file_dict["volatility"] = ""
        if(elem["name"].lower() == "consistency"):
            if("force" in elem):
                if (elem["force"].lower() == "true"):
                    name_list[-1] = name_list[-1] + "-R"

    config_file_dict["des_dimensions"] = ";".join(name_list)
    config_file_dict["granularity"] = ";".join(granularity_list)

    # generating 11 line of the config file
    cons_list = []
    for elem in json["add_con_rules"]:
        antecedent_string = ",".join(elem["antecedent"])
        conseguent_string = ",".join(elem["consequent"])
        cons_list.append(antecedent_string + ":" + conseguent_string)
    config_file_dict["add_con_rules"] = ";".join(cons_list)
    # generating 14 line of the config file
    config_file_dict["con_files"] = ";".join(json["con_rules"])

    data = (config_file_dict["input"] + "\n"
            + config_file_dict["output"] + "\n"
            + config_file_dict["sel_attributes"] + "\n"
            + config_file_dict["g_attributes"] + "\n"
            + config_file_dict["values_intervals"] + "\n"
            + config_file_dict["selected_values"] + "\n"
            + config_file_dict["des_dimensions"] + "\n"
            + config_file_dict["volatility"] + "\n"
            + config_file_dict["mean_acc"] + "\n"
            + config_file_dict["interval_acc"] + "\n"
            + config_file_dict["add_con_rules"] + "\n"
            + config_file_dict["granularity"] + "\n"
            + config_file_dict["source_summary"] + "\n"
            + config_file_dict["con_files"] + "\n")

    filename = '/temp_config_files/%s.txt' % (file_id)
    hdfs_filename = '%s/%s.txt' % (HDFSCONFIGDIR, file_id)
    f = open(filename, 'w')
    f.write(data)
    f.close()
    save_to_hdfs(filename, hdfs_filename)
    return file_id, True

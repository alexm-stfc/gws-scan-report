import argparse
import pathlib

import elasticsearch_dsl as esd
import humanize
import matplotlib
import matplotlib.backends.backend_pdf
import matplotlib.pyplot as plt
import pandas as pd
import tomli
from gws_volume_scanner.client import queries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", help="Path to configuration file.")
    parser.add_argument("path", help="The path you would like a report on.")
    parser.add_argument(
        "output", help="The location you would like the output to be saved."
    )
    parser.add_argument("--depth", default=1, type=int, help="How deep to go.")
    parser.add_argument("--type", default="pdf", type=str, help="What type of output")
    parser.add_argument("--brief", default=False, type=bool, help="Brief output excluding filetypes and some other information")
    args = parser.parse_args()
    path = pathlib.Path(args.config_file).resolve().expanduser()
    with open(path, "rb") as thefile:
        toml_dict = tomli.load(thefile)

    esd.connections.create_connection(
        alias="default",
        hosts=toml_dict["es_hosts"],
        timeout=20,
        verify_certs=False,
        headers={"x-api-key": toml_dict["es_api_key"]},
    )
    queue = [args.path]
    base = queue[0].count("/")
    depth = args.depth

    if args.type == 'pdf':
        with matplotlib.backends.backend_pdf.PdfPages(args.output) as pdf:
            while queue:
                path = queue.pop()
                path.count("/")
                if (path.count("/") - base) < depth:
                    data = tree(path, toml_dict)
                    childs = [
                        f'{path}/{x["path"]}'
                        for x in data["children"]
                        if x["path"] != "__unindexed_children__"
                    ]
                    queue.extend(childs)
                    draw_row(pdf, data)
    elif args.type == 'csv':
        with open(args.output, 'w') as out_file:
            while queue:
                path = queue.pop()
                path.count("/")
                if (path.count("/") - base) < depth:
                    data = tree(path, toml_dict)
                    childs = [
                        f'{path}/{x["path"]}'
                        for x in data["children"]
                        if x["path"] != "__unindexed_children__"
                    ]
                    queue.extend(childs)
                    to_csv(out_file, data, args.brief)

def to_csv(fo, data, brief=False):
    try:
        children = pd.DataFrame(data["children"])
        children.sort_values("path", inplace=True)
        children = children[children["size"] > 0]

        children["size_human"] = [humanize.naturalsize(x) for x in children["size"]]
        children["count_human"] = [humanize.intword(x) for x in children["count"]]
        children.drop(
            labels=["es_doc_count", "mean_heat", "count", "size"], axis=1, inplace=True
        )

        users = pd.DataFrame(data["users"])
        users = users.transpose()
        #users.sort_values("size", inplace=True)
        users.sort_index(inplace=True)
        users["size_human"] = [humanize.naturalsize(x) for x in users["size"]]
        users["count_human"] = [humanize.intword(x) for x in users["count"]]
        users.drop(labels=["count", "size"], axis=1, inplace=True)

        filetypes = pd.DataFrame(data["filetypes"])
        filetypes = filetypes.transpose()
        filetypes.sort_values("size", inplace=True)
        filetypes["size_human"] = [humanize.naturalsize(x) for x in filetypes["size"]]
        filetypes["count_human"] = [humanize.intword(x) for x in filetypes["count"]]
        filetypes.drop(labels=["count", "size"], axis=1, inplace=True)
        filetypes.reset_index(inplace=True)

        heat = pd.DataFrame(data["heat"])
        heat = heat.transpose()
        #heat.sort_values("size", inplace=True)
        heat.sort_index(inplace=True)
        heat["size_human"] = [humanize.naturalsize(x) for x in heat["size"]]
        heat["count_human"] = [humanize.intword(x) for x in heat["count"]]
        heat.drop(labels=["count", "size"], axis=1, inplace=True)

        del data["children"]
        del data["users"]
        del data["filetypes"]
        del data["heat"]

        data["total_size"] = humanize.naturalsize(data["total_size"])
        data["total_count"] = humanize.intword(data["total_count"])
        toplevel = pd.DataFrame(data, index=[0])
        if brief:
            fo.write('At level {}\n'.format(toplevel['path'].values[0]))
            fo.write('Children\n')
            children.to_csv(fo)
            fo.write('\n')

            fo.write('Users\n')
            users.to_csv(fo)
            fo.write('\n')

            fo.write('Heat (Last access time using atime)\n')
            heat.to_csv(fo)
            fo.write('\n\n')
        else:
            fo.write('Top level\n')
            toplevel.to_csv(fo)
            fo.write('\n')

            fo.write('Children\n')
            children.to_csv(fo)
            fo.write('\n')

            fo.write('Users\n')
            users.to_csv(fo)
            fo.write('\n')

            fo.write('Filetype\n')
            filetypes.to_csv(fo)
            fo.write('\n')

            fo.write('Heat (Last access time using atime)\n')
            heat.to_csv(fo)
            fo.write('\n')
    
    except (KeyError, IndexError):
        pass


def draw_row(pdf, data):
    try:
        children = pd.DataFrame(data["children"])
        children.sort_values("size", inplace=True)
        children = children[children["size"] > 0]

        children["size_human"] = [humanize.naturalsize(x) for x in children["size"]]
        children["count_human"] = [humanize.intword(x) for x in children["count"]]
        children.drop(
            labels=["es_doc_count", "mean_heat", "count", "size"], axis=1, inplace=True
        )

        users = pd.DataFrame(data["users"])
        users = users.transpose()
        users.sort_values("size", inplace=True)
        users["size_human"] = [humanize.naturalsize(x) for x in users["size"]]
        users["count_human"] = [humanize.intword(x) for x in users["count"]]
        users.drop(labels=["count", "size"], axis=1, inplace=True)

        filetypes = pd.DataFrame(data["filetypes"])
        filetypes = filetypes.transpose()
        filetypes.sort_values("size", inplace=True)
        filetypes["size_human"] = [humanize.naturalsize(x) for x in filetypes["size"]]
        filetypes["count_human"] = [humanize.intword(x) for x in filetypes["count"]]
        filetypes.drop(labels=["count", "size"], axis=1, inplace=True)
        filetypes.reset_index(inplace=True)

        heat = pd.DataFrame(data["heat"])
        heat = heat.transpose()
        heat.sort_values("size", inplace=True)
        heat["size_human"] = [humanize.naturalsize(x) for x in heat["size"]]
        heat["count_human"] = [humanize.intword(x) for x in heat["count"]]
        heat.drop(labels=["count", "size"], axis=1, inplace=True)

        del data["children"]
        del data["users"]
        del data["filetypes"]
        del data["heat"]

        data["total_size"] = humanize.naturalsize(data["total_size"])
        data["total_count"] = humanize.intword(data["total_count"])
        toplevel = pd.DataFrame(data, index=[0])

        fig, (toptab, treetab, usertab, typetab, heattab) = plt.subplots(
            nrows=5, figsize=(8.3, 11.7)
        )

        pd.plotting.table(toptab, toplevel, loc="center")
        toptab.axis("off")

        pd.plotting.table(treetab, children, loc="center")
        treetab.axis("off")

        pd.plotting.table(usertab, users, loc="center")
        usertab.axis("off")

        pd.plotting.table(typetab, filetypes, loc="center")
        typetab.axis("off")

        pd.plotting.table(heattab, heat, loc="center")
        heattab.axis("off")

        pdf.savefig()
        plt.close()
    except (KeyError, IndexError):
        plt.close()


def tree(path, config):
    scan_id = queries.latest_scan_id(path, config["status_index"])
    if scan_id is None:
        raise ValueError("There is no scan for the path you gave.")

    result = queries.children(path, config["index"], scan_id)

    children = result["aggregations"]["path"]["children"]["buckets"]
    children = [
        {
            "path": x["key"],
            "count": x["count"]["value"],
            "size": x["size"]["value"],
            "es_doc_count": x["doc_count"],
            "mean_heat": round(x["mean_heat"]["value"]),
        }
        for x in children
    ]

    total_size = result["aggregations"]["path"]["size"]["value"]
    total_count = result["aggregations"]["path"]["count"]["value"]

    # Unindexed /files/ in a folder will not be included in result.
    # Add them in manually.
    sum_size_children = sum([x["size"] for x in children])
    sum_count_children = sum([x["count"] for x in children])

    balance_size = total_size - sum_size_children
    balance_count = (total_count - 1) - sum_count_children

    if balance_count > 0:
        children.append(
            {
                "path": "__unindexed_children__",
                "count": balance_count,
                "size": balance_size,
                "es_doc_count": 0,
                "mean_heat": 0,
            }
        )

    file_heats = {k: v for k, v in heat(path, scan_id, config).items() if v["count"]}
    file_types = {k: v for k, v in types(path, scan_id, config).items() if v["count"]}

    response = {
        "path": path,
        "children": children,
        "total_size": total_size,
        "total_count": total_count,
        "users": users(path, scan_id, config),
        "filetypes": file_types,
        "heat": file_heats,
        "scan_id": scan_id,
    }
    return response


def heat(path, scan_id, config):
    """Query elasticsearch for file heat information below a given path."""
    heat_q = queries.hotness(path, config["index"], scan_id)
    results = {}
    for hot in heat_q["aggregations"]["counts"].keys():
        if hot != "doc_count":
            results[hot] = {
                "size": heat_q["aggregations"]["sizes"][hot]["value"],
                "count": heat_q["aggregations"]["counts"][hot]["value"],
            }
    results = {k: v for k, v in results.items() if v["count"] > 0}
    return results


def types(path, scan_id, config):
    """Query elasticsearch for filetype information below a given path."""
    types_q = queries.filetypes(path, config["index"], scan_id)
    results = {}
    for type_ in types_q["aggregations"]["counts"].keys():
        if type_ != "doc_count":
            results[type_] = {
                "size": types_q["aggregations"]["sizes"][type_]["value"],
                "count": types_q["aggregations"]["counts"][type_]["value"],
            }
    results = {k: v for k, v in results.items() if v["count"] > 0}
    return results


def users(path, scan_id, config):
    """Query elasticsearch for user information below a given path."""
    users_q = queries.users(path, config["index"], scan_id)
    results = {}
    for user in users_q["aggregations"]["counts"].keys():
        if user != "doc_count":
            results[user] = {
                "size": users_q["aggregations"]["sizes"][user]["value"],
                "count": users_q["aggregations"]["counts"][user]["value"],
            }
    results = {k: v for k, v in results.items() if v["count"] > 0}
    return results

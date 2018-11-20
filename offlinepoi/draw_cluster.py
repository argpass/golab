#!coding: utf-8
import random
from collections import defaultdict

from matplotlib import pyplot as plt
from matplotlib.colors import cnames
from matplotlib.markers import MarkerStyle


def draw_scatter(rows, x_idx, y_idx):
    x_range = [row[x_idx] for row in rows]
    y_range = [row[y_idx] for row in rows]
    plt.figure(1)
    plt.xlabel("label_x")
    plt.ylabel("label_y")
    plt.title("xx title")
    plt.figure(2)
    plt.scatter(x_range, y_range)
    plt.legend("hello legend")
    plt.show()


field_converters = {
    "cluster": lambda x: int(x),
    "lid": lambda x: int(x),
    "density": lambda x: float(x),
    "dis": lambda x: float(x),
    "lat": lambda x: float(x),
    "lng": lambda x: float(x),
}


def draw(path):
    fields_idx = dict()
    fields = []
    clusters = defaultdict(list)
    rows = []
    with open(path) as fi:
        for line_no, line in enumerate(fi.readlines()):
            line = line.replace("\n", "")
            if line_no == 0:
                fields = line.split(",")
                for idx, name in enumerate(fields):
                    fields_idx[name] = idx
                    if name not in field_converters:
                        raise ValueError(u"un supported name `%s`" % name)
            else:
                values = line.split(",")
                vals = []
                for idx, val in enumerate(values):
                    vals.append(field_converters[fields[idx]](val))
                rows.append(vals)
                cluster_lid = vals[fields_idx["cluster"]]
                clusters[cluster_lid].append(vals)
    plt.figure(1)
    # plt.subplot(211)
    # plt.hist([row[fields_idx["density"]] for row in rows], bins=100)
    # plt.subplot(211)
    plt.scatter(
        [row[fields_idx["density"]] for row in rows],
        [row[fields_idx["dis"]] for row in rows],
        marker="o", s=30, color='b', label='density-dis'
    )
    plt.figure(2)
    # plt.subplot(212)
    colors = cnames.keys()
    markers = MarkerStyle.markers.keys()
    for cluster_lid, rows in clusters.items():
        print "draw cluster:", cluster_lid, len(rows)
        c = colors[random.randint(0, len(colors) - 1)] if cluster_lid != 0 \
            else 'r'
        m = markers[random.randint(0, len(markers) - 1)] if cluster_lid != 0 \
            else '.'
        plt.scatter(
            [row[fields_idx["lng"]] for row in rows],
            [row[fields_idx["lat"]] for row in rows],
            label=str(cluster_lid),
            c=c,
            marker=m,
        )
    plt.show()


def options_gen():
    for marker, color in [("x", "r"), ("+", "b"), ("o", "m"), ("*", "g"), ("0", "r"), ("o", "g")]:
        yield {"marker": marker, "color": color}


if __name__ == '__main__':
    draw("/tmp/density.csv")


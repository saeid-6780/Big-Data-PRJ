import random

import pandas as pd
import seaborn as sb
import folium
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from yellowbrick.cluster import KElbowVisualizer
from sklearn.utils import shuffle
import numpy as np
import matplotlib.cm as cm
import matplotlib.colors as colors

df = pd.read_csv("data.csv")

train_count = int(0.2 * len(df))
train_data = df[:train_count]
values_for_clustering = train_data[['Lat', 'Lon']]
print(values_for_clustering.dtypes)

model = KMeans()
visualizer = KElbowVisualizer(model, k=(1, 10))
visualizer.fit(values_for_clustering)
visualizer.show(outpath="out/kelbow_minibatchkmeans.png")
visualizer.show()

best_clusters_count = visualizer.elbow_value_
kmeans = KMeans(n_clusters=best_clusters_count, random_state=0)
kmeans.fit(values_for_clustering)
centroids_k = kmeans.cluster_centers_

clocation_k = pd.DataFrame(centroids_k, columns=['Latitude', 'Longitude'])
plt.scatter(clocation_k['Latitude'], clocation_k['Longitude'], marker="x", s=200)
centroid_k = clocation_k.values.tolist()
plt.savefig("out/centroids.png")
plt.show()

map_k = folium.Map(location=[clocation_k['Latitude'].mean(), clocation_k['Longitude'].mean()], zoom_start=10)
for point in range(0, len(centroid_k)):
    folium.Marker(centroid_k[point], popup=centroid_k[point]).add_to(map_k)
map_k.save("out/map.html ")

label_k = kmeans.labels_
df_new_k = train_data.copy()
df_new_k['Clusters'] = label_k
# print(df_new_k)

sb.catplot(data=df_new_k, x="Clusters", kind="count", height=7, aspect=2)
plt.savefig("out/factorplot.png")
plt.show()
plt.scatter(df_new_k['Lat'], df_new_k['Lon'], c=label_k, cmap='viridis');
plt.savefig("out/scatter.png")
plt.show()

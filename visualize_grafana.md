# Steps to make grafana visualization
 - Access ```localhost:3000```, username and password are both "admin". Click on 'Datasources' -> "PostgreSQL" -> type the config below -> Click 'Save & test'
 - Copy the uid in red


<div style="display: flex; flex-direction: column;">
<img src=/Assets/postgres-grafana.png alt = "postgres_grafana">

<p style="text-align: center;"> Change PostgreSQL Connection only </p>
</div>

 - Choose import dashboard  from the search bar, navigate to folder ```src/Grafana``` and choose 'dashboard.json'
 - After this, the dashboard will display no data, this is due to mismatch of datasource uid. Click on the setting icon, Choose <b> Json Model </b>, then replace all uids within all datasources to the copied text before (You can do it quickly with ctrl + F -> replace)

<div style="display: flex; flex-direction: column;">
<img src=/Assets/Replace_datasource_uid.png alt = "postgres_grafana">

<p style="text-align: center;"> Replace datasource uid </p>
</div>

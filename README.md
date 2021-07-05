<p>
    <h3>Step 1. Up preparing</h3>
    First of all we should create bridge and build docker.<br>
    <code>docker network create container_bridge</code><br>
    <code>docker build -t docker_selenium -f selenium\Dockerfile .</code>
    Build docker-composer<br>
    <code>docker-compose build</code><br>
</p>
<p>
    <h3>Step 2. Get access to worker</h3>
    <code>docker exec -ti -u root "worker-container-name" bash</code><br>
    <code>sudo chmod 777 /var/run/docker.sock</code>
</p>
<p>
    <h3>Step 3. Initialize variables</h3>
    Go to Airflow admin panel on 8080 port.<br>
    <strong>username: airflow<br> password: airflow</strong><br>
    Open "Variable" tab and add:<br>
    <ul>
    <li>
        <strong>MAP_API_KEY<strong> - api key for work with geocoding
         and routing map service. Service locationiq.com allow
          make 5000 requests per day, but you can do only 2 requests per second.
    </li>
    <li>
        <strong>mongo_db_flats_collection_name<strong> -
         collection name in database.
    </li>
    <li>
        <strong>mongo_db_flats_database_name<strong> - database name.
    </li>
    <li>
        <strong>RUSSIAN_CITIES<strong> - json object in format:<br>
        <code>
        {<br>
            city_name: { <br>
            "lat": str,<br> 
            "lng": str,<br>
            "region_idx": int, <br>
            "parse": bool <br>
        }
        </code>
        city_name - name of city <br>
        lat - latitude of city center <br>
        lng - longitude of city center <br>
        region_idx - number, which you can find in url parameter "region" <br>
        parse - boolean value, if true this city will be parsed <br>
    </li>
    </ul>
</p>
<p>
    <h3>Step 4. Create connection</h3>
    Open "Connections" tab and add new mongodb connection to database<br>
</p>
from flask import Flask, render_template, request,session,jsonify,Response,redirect,send_file,url_for
import json
import requests
from fuzzywuzzy import fuzz
import re
from datetime import datetime
import PEPCheck
import test
import csv
import io
from io import StringIO
import pandas as pd
import identity
import identity.web
from flask_session import Session
import app_config
 
app = Flask(__name__)
app.config.from_object(app_config)
Session(app)
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
auth = identity.web.Auth(
    session=session,
    authority=app.config.get("AUTHORITY"),
    client_id=app.config["CLIENT_ID"],
    client_credential=app.config["CLIENT_SECRET"],
)

@app.route("/login")
def login():
    return render_template("login.html", version=identity.__version__, **auth.log_in(
        scopes=app_config.SCOPE, # Have user consent to scopes during log-in
        redirect_uri=url_for("auth_response", _external=True), # Optional. If present, this absolute URL must match your app's redirect_uri registered in Azure Portal
        ))

@app.route(app_config.REDIRECT_PATH)
def auth_response():
    result = auth.complete_log_in(request.args)
    if "error" in result:
        return render_template("auth_error.html", result=result)
    return redirect(url_for("index"))

@app.route("/logout")
def logout():
    return redirect(auth.log_out(url_for("index", _external=True)))
# Route to render the upload page (GET method)

@app.route('/upload')
def upload_file():
    if not (app.config["CLIENT_ID"] and app.config["CLIENT_SECRET"]):
        # This check is not strictly necessary.
        # You can remove this check from your production code.
        return render_template('config_error.html')
    if not auth.get_user():
        return redirect(url_for("login"))
    return render_template('upload.html')

@app.route('/upload_form', methods=['POST'])
def handle_file():
    # start_time = time.time()  # Start time
    if not auth.get_user():
        return redirect(url_for("login"))
    else:
        if 'file' not in request.files:
            return redirect(request.url)

        file = request.files['file']

        if file:
            # Check if the file is in CSV or Excel format
            if file.filename.endswith('.csv'):
                data = pd.read_csv(file)
            else:
                data = pd.read_excel(file)

            print("Data loaded successfully:")
            print(data.head())  # Display input data once

            print(f"Columns in Excel file: {data.columns}")

            data = data.fillna('')

            # Create a list to hold rows for the final DataFrame
            output_rows = []

            for index, row in data.iterrows():
                search_name = row.get("name")
                # threshold = int(row.get("threshold", 80))
                threshold_value = row.get("threshold", 80) 

                try:
                    threshold = int(float(threshold_value))
                except (ValueError, TypeError):
                    threshold = 80 

                search_country = row.get("country")
                # Ensure dob is a valid string or integer, without decimals
                #search_dob = str(int(float(row.get("dob", 0)))) if row.get("dob") else ""
                dob_value = str(row.get("dob", "")).strip() 
                if dob_value:  
                    try:
                        search_dob = datetime.strptime(dob_value, "%d-%b-%Y").strftime("%d-%m-%Y")  
                    except ValueError:
                        search_dob = ""  # Handle invalid date formats  
                else:
                    search_dob = ""


                search_address = row.get("address")
                search_type = row.get("type")
                search_number = row.get("number")
                search_email = row.get("email")


                print(f"Processing row {index} data:")
                print(f"search_name={search_name}, threshold={threshold}, search_country={search_country}, search_dob={search_dob}, search_address={search_address}")

                # Fetch and search for sanctions results
                sanctions_results = test.fetch_and_search(
                    search_name=search_name,
                    threshold=threshold,
                    search_country=search_country,
                    search_dob=search_dob,
                    search_address=search_address,
                    search_type=search_type,
                    search_number=search_number,
                    search_email=search_email
                )

                # Fetch and search for PEP results
                position = row.get("position",'')
                name= row.get("name", "")
                # dob = str(int(float(row.get("dob", 0)))) if row.get("dob") else None
                dob_pep = row.get("dob", "")

                
                if isinstance(dob_pep, datetime):
                    dob_pep = dob_pep.strftime("%d-%m-%Y")  

                dob_pep = str(dob_pep).strip()

                if dob_pep:
                    try:
                        # "dd-Sep-yyyy"
                        dob = datetime.strptime(dob_pep, "%d-%b-%Y").strftime("%Y-%m-%d")
                    except ValueError:
                        try:
                            #  dd-mm-yyyy
                            dob = datetime.strptime(dob_pep, "%d-%m-%Y").strftime("%Y-%m-%d")
                        except ValueError:
                            try:
                                # yyyy-mm-dd
                                dob = datetime.strptime(dob_pep, "%Y-%m-%d").strftime("%Y-%m-%d")
                            except ValueError:
                                dob = ""  
                else:
                    dob = ""

                country = row.get("country") or None

                print(f'pep name{name},pep position{position}')
                pep_results = []
                pep_data = {}
                if name and not position:
                    pep_data['matched_names'] = PEPCheck.search_names(name, threshold,  dob=dob, country=country)
                if position and not name:
                    print('position')
                    pep_data['matched_positions'] = PEPCheck.search_positions(position, threshold, dob=dob, country=country)
                if name and position:
                    print('name and position')
                    pep_data['matched_name_positions'] = PEPCheck.search_name_and_position(name, position, threshold, dob=dob, country=country)

                for result_list in pep_data.values():
                    pep_results.extend(result_list)

                # Remove duplicates from PEP results
                seen_pep = set()
                unique_pep_results = []
                for result in pep_results:
                    unique_identifier = (result.get('name'), result.get('position'), result.get('startDate'), result.get('endDate'))
                    if unique_identifier not in seen_pep:
                        seen_pep.add(unique_identifier)
                        unique_pep_results.append(result)

                unique_pep_results = sorted(unique_pep_results, key=lambda x: x.get('score', 0), reverse=True)

                # Process sanctions results
                if not sanctions_results:
                    new_row = row.to_dict()
                    new_row["Name Sanction Check"] = "None"
                    new_row["Sanction Source"] = ""
                    new_row["Sanction Name Match Score"] = ""
                    new_row["Details"] = ""
                    output_rows.append(new_row)
                else:
                    for result_index, result in enumerate(sanctions_results):
                        if result_index == 0:
                            new_row = row.to_dict()
                        else:
                            new_row = {col: '' for col in data.columns}
                        if isinstance(result, dict):
                            new_row["Name Sanction Check"] = result.get("Name", "")
                            new_row["Sanction Source"] = result.get("Source", "")
                            new_row["Sanction Name Match Score"] = result.get("Name_score", "")
                            excluded_keys = ['Name', 'Source', 'Name_score']
                            new_row["Details"] = "\n".join(
                                f"{key}: {value}" for key, value in result.items() if key not in excluded_keys
                            )
                        else:
                            continue
                        output_rows.append(new_row)

                # Process PEP results
                if not unique_pep_results:
                    new_row = row.to_dict()
                    new_row["PEP Name Check"] = "None"
                    new_row["PEP Score"] = ""
                    output_rows.append(new_row)
                else:
                    for result_index, result in enumerate(unique_pep_results):
                        if result_index == 0:
                            new_row = row.to_dict()
                        else:
                            new_row = {col: '' for col in data.columns}
                        new_row["PEP Name Check"] = result.get("name", "")
                        new_row["PEP Score"] = result.get("score", "")
                        output_rows.append(new_row)
                # print(unique_pep_results)

            # Convert the list of rows to a DataFrame
            output_df = pd.DataFrame(output_rows)

            # Convert the updated data to CSV
            output = io.StringIO()
            output_df.to_csv(output, index=False)
            output.seek(0)

            return Response(output, mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=processed_data.csv"})

        return jsonify({"error": "No file selected or invalid file format."}), 400

@app.route("/", methods=['GET', 'POST'])
def index():
    if not (app.config["CLIENT_ID"] and app.config["CLIENT_SECRET"]):
        # This check is not strictly necessary.
        # You can remove this check from your production code.
        return render_template('config_error.html')
    if not auth.get_user():
        return redirect(url_for("login"))
    return render_template('index.html', user=auth.get_user(), version=identity.__version__)


@app.route("/call_downstream_api")
def call_downstream_api():
    token = auth.get_token_for_user(app_config.SCOPE)
    if "error" in token:
        return redirect(url_for("login"))
    # Use access token to call downstream api
    api_result = requests.get(
        app_config.ENDPOINT,
        headers={'Authorization': 'Bearer ' + token['access_token']},
        timeout=30,
    ).json()
    return render_template('index.html', result=api_result)

'''@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')'''
 
 
@app.route('/pep', methods=['GET', 'POST'])
def pep():
    if not (app.config["CLIENT_ID"] and app.config["CLIENT_SECRET"]):
        # This check is not strictly necessary.
        # You can remove this check from your production code.
        return render_template('config_error.html')
    if not auth.get_user():
        return redirect(url_for("login"))
    else:
        matched_results = []  # To store matched records
        search_type = None  # Either 'positions' or 'names'
        if request.is_json:
            data = request.json
            name = data.get("name")
            dob = data.get("dob")  
            position = data.get("position")
            threshold = data.get("threshold", 80)  # Default to 80 if threshold is None
            country=data.get('country')
            results = {}

            if name:
                matched_names = PEPCheck.search_names(name, threshold, dob,country)
                results['matched_names'] = matched_names
            if position:
                matched_positions = PEPCheck.search_positions(position, threshold, dob,country)
                results['matched_positions'] = matched_positions
            if name and position:
                matched_name_positions = PEPCheck.search_name_and_position(name, position, threshold, dob,country)
                results['matched_name_positions'] = matched_name_positions

            combined_results = []
            for result_list in results.values():
                combined_results.extend(result_list)

            # Remove duplicates based on 'name' and 'position' 
            seen = set()
            unique_results = []
            for result in combined_results:
                unique_identifier = (result.get('name'),result.get('position'),result.get('startDate'),result.get('endDate'))  
                if unique_identifier not in seen:
                    seen.add(unique_identifier)
                    unique_results.append(result)

            # Sort the results in descending order
            unique_results = sorted(unique_results, key=lambda x: x.get('score', 0), reverse=True)

            return jsonify({"Response": unique_results})
        else:
            if request.method == 'POST':
                page = 1
                search_type = request.form.get('search_type')
                threshold = request.form.get('threshold') or 80
                dob = request.form.get('dob')
                country=request.form.get('country') or 'India'

                if search_type == 'names_positions':
                    name_query = request.form.get('name_query') or "" 
                    position_query = request.form.get('position_query') or ""  
                    query = None 
                else:
                    query = request.form.get('name_query') or request.form.get('position_query')
                    name_query = ""
                    position_query = ""
            else:
                page = int(request.args.get('page', 1))
                search_type = request.args.get('search_type')
                threshold = request.args.get('threshold') or 80
                dob = request.args.get('dob')
                country=request.args.get('country')

                if search_type == 'names_positions':
                    name_query = request.args.get('name_query') or "" 
                    position_query = request.args.get('position_query') or ""  
                    query = None
                else:
                    query = request.args.get('query')
                    name_query = ""
                    position_query = ""

            # Ensure threshold is an integer
            try:
                threshold = int(threshold)
            except ValueError:
                threshold = 80

            matched_results = []

            # Perform search based on the search type
            if search_type == 'names':
                all_results = PEPCheck.search_names(query, threshold, dob,country)
            elif search_type == 'positions':
                all_results = PEPCheck.search_positions(query, threshold, dob,country)
            elif search_type == 'names_positions':
                all_results = PEPCheck.search_name_and_position(name_query, position_query, threshold, dob,country)
            else:
                all_results = []

            # Remove duplicates based on 'name' and 'position' 
            seen = set()
            unique_results = []
            for result in all_results:
                unique_identifier = (result.get('name'), result.get('position'),result.get('startDate'),result.get('endDate'))  
                if unique_identifier not in seen:
                    seen.add(unique_identifier)
                    unique_results.append(result)

            # Sort the results in descending order
            unique_results = sorted(unique_results, key=lambda x: x.get('score', 0), reverse=True)

            # Paginate results
            per_page = 20
            total_results = len(unique_results)
            total_pages = (total_results + per_page - 1) // per_page
            start = (page - 1) * per_page
            end = start + per_page
            matched_results = unique_results[start:end]
            has_next = end < total_results

            # print(matched_results)

            # Render template
            return render_template(
                'pep.html',
                results=matched_results,
                search_type=search_type,
                query=query,
                threshold=threshold,
                dob=dob,
                country=country,
                page=page,
                has_next=has_next,
                name_query=name_query,
                position_query=position_query,
                total_pages=total_pages 
            )
        
@app.route('/sanction',methods=['GET','POST'])
def sanction():
    if not (app.config["CLIENT_ID"] and app.config["CLIENT_SECRET"]):
        # This check is not strictly necessary.
        # You can remove this check from your production code.
        return render_template('config_error.html')
    if not auth.get_user():
        return redirect(url_for("login"))
    else:
        if request.is_json:       
            data = request.json
            search_name = data.get("name")
            threshold = int(data.get("threshold", 80))  # Default to 60 if not provided
            search_country = data.get("country")
            search_dob = data.get("dob")
            search_address = data.get("address")
            search_type = data.get("type")
            search_number=data.get("number")
            search_email=data.get("email")
                #  `fetch_and_search` function
            results = test.fetch_and_search(
                search_name=search_name,
                threshold=threshold,
                search_country=search_country,
                search_dob=search_dob,
                search_address=search_address,
                search_type=search_type,
                search_number=search_number,
                search_email=search_email
            )
                # JSON response
            return jsonify(results), 200
        else: 
            if request.method == 'POST':
                page = 1
                search_type = request.form.get("search_type")
                search_name = request.form.get("name")
                threshold = int(request.form.get("threshold", 80))  # Default to 80 if not provided
                search_country = request.form.get("country")
                search_dob = request.form.get("dob")
                search_address = request.form.get("address")
                search_number=request.form.get('passport')
                search_email=request.form.get('email')
            else:
                page = int(request.args.get('page', 1))
                search_type = request.args.get("search_type")
                search_name = request.args.get("name")
                threshold = int(request.args.get("threshold", 80))  # Default to 80 if not provided
                search_country = request.args.get("country")
                search_dob = request.args.get("dob")
                search_address = request.args.get("address")
                search_number=request.args.get('passport')
                search_email=request.args.get('email')

                # `fetch_and_search` function
            results = test.fetch_and_search(
                search_name=search_name,
                threshold=threshold,
                search_country=search_country,
                search_dob=search_dob,
                search_address=search_address,
                search_type=search_type,
                search_number=search_number,
                search_email=search_email
            )
                # Debugging: Check the structure of the results
                # print("Fetched Results:", results)

                # Sort by 'score' in descending order (if 'score' is missing, default to 0)
            results = sorted(results or [], key=lambda x: int(x.get('Name_score', 0)), reverse=True)
            per_page = 20
            total_results = len(results)
            total_pages = (total_results + per_page - 1) // per_page  # Calculate total pages
            start = (page - 1) * per_page
            end = start + per_page
            matched_results = results[start:end]
            has_next = end < total_results

            return render_template(
                'sanction.html',
                results=matched_results,
                search_name=search_name,
                search_type=search_type,
                threshold=threshold,
                search_country=search_country,
                search_dob=search_dob,
                search_address=search_address,
                search_number=search_number,
                page=page,
                has_next=has_next,
                total_pages=total_pages
            )
@app.route('/pep/download', methods=['GET'])
def download_results():
    if not (app.config["CLIENT_ID"] and app.config["CLIENT_SECRET"]):
        # This check is not strictly necessary.
        # You can remove this check from your production code.
        return render_template('config_error.html')
    if not auth.get_user():
        return redirect(url_for("login"))
    else:
        search_type = request.args.get('search_type')
        query = request.args.get('query')
        threshold = request.args.get('threshold', 80)
        dob = request.args.get('dob')
        
        # Ensure threshold is an integer
        try:
            threshold = int(threshold)
        except ValueError:
            threshold = 80

        # Retrieve the results based on the search type and query
        matched_results = []

        if search_type == 'names':
            matched_results = PEPCheck.search_names(query, threshold, dob)
        elif search_type == 'positions':
            matched_results = PEPCheck.search_positions(query, threshold, dob)
        elif search_type == 'names_positions':
            name_query = request.args.get('name_query', "")
            position_query = request.args.get('position_query', "")
            matched_results = PEPCheck.search_name_and_position(name_query, position_query, threshold, dob)

        seen = set()
        unique_results = []
        for result in matched_results:
            unique_identifier = (result.get('name'), result.get('position'),result.get('startDate'),result.get('endDate'))  # Change this to a suitable identifier
            if unique_identifier not in seen:
                seen.add(unique_identifier)
                unique_results.append(result)

        # Sort the results in descending order
        unique_results = sorted(unique_results, key=lambda x: x.get('score', 0), reverse=True)

        # Create a CSV file from the results
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['Name', 'Position', 'Score Accuracy', 'DOB', 'Position Start Date', 'Position End Date', 'Record'])  # CSV Header

        for result in unique_results :
            writer.writerow([result.get('name', '-'), 
                            result.get('position', '-'), 
                            result.get('score', '-'), 
                            result.get('dob', '-'), 
                            result.get('startDate', '-'), 
                            result.get('endDate', '-'), 
                            result.get('record', '-')])
        
        output.seek(0)  # Go to the beginning of the StringIO object
        if search_type=='names' or search_type=='positions':
            filename = f"pep_results_{query}.csv"
        elif search_type =='names_positions':
            filename = f"pep_results_{name_query}_{position_query}.csv"
        else:
            filename = "pep_results_default.csv" 


        # Send the CSV data as a response
        return Response(
            output,
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename={filename}'
            }
        )
@app.route('/sanction/download', methods=['GET'])
def download_sanction():
    if not (app.config["CLIENT_ID"] and app.config["CLIENT_SECRET"]):
        # This check is not strictly necessary.
        # You can remove this check from your production code.
        return render_template('config_error.html')
    if not auth.get_user():
        return redirect(url_for("login"))
    else:
        search_type = request.args.get("search_type")
        search_name = request.args.get("name")
        threshold = int(request.args.get("threshold", 80))  # Default to 80 if not provided
        search_country = request.args.get("country")
        search_dob = request.args.get("dob")
        search_address = request.args.get("address")
        print(search_type,search_name,threshold,search_country,search_dob,search_address)

        try:
            threshold=int(threshold)
        except ValueError:
            threshold=80
        results=[]
                # `fetch_and_search` function
        results= test.fetch_and_search(
            search_name=search_name,
            threshold=threshold,
            search_country=search_country,
            search_dob=search_dob,
            search_address=search_address,
            search_type=search_type
        )
        # Sort by 'score' in descending order (if 'score' is missing, default to 0)
        results = sorted(results or [], key=lambda x: int(x.get('Name_score', 0)), reverse=True)
        # print(results)
        # print(request.args)
        output=StringIO()

        writer=csv.writer(output)
        writer.writerow(['Name','Score Accuracy','Source','Address','Alias Name','Date Of Birth','Place Of Birth','Country'])

        for result in results:
            writer.writerow([result.get('Name', '-'), 
                            result.get('Name_score', '-'), 
                            result.get('Source', '-'), 
                            result.get('Address', '-'), 
                            result.get('Alias_name', '-'), 
                            result.get('Date_of_Birth', '-'), 
                            result.get('Birth_Place', '-'),
                            result.get('Nationality','-')])
        
        output.seek(0)
        filename='sanction results.csv'

        return Response(
            output,
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename={filename}'
            }
        )

# if __name__ == '__main__':
#     app.run(debug=True)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

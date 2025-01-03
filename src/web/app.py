import os
import sys

# Add the src directory to Python's path
# This allows us to import modules from the 'src' directory located at the parent level
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(src_path)

from flask import Flask, request, jsonify, send_from_directory
import pdfplumber
from producer.producer import produce_resume
from db.database import store_resume

# Initialize the Flask web application
app = Flask(__name__)

# Configure the upload folder where resumes will be stored and allowed file extensions (just pdf here)
UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"pdf"}
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Ensure the upload folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


# Function to check if the file has an allowed extension
def allowed_file(filename):
    """
    Check if the file has an allowed extension (PDF).

    Args:
    filename (str): The name of the file being uploaded.

    Returns:
    bool: True if the file has an allowed extension, False otherwise.
    """
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def extract_skills_from_text(text):
    """
    Extract skills from the resume text based on a predefined list of skills.

    Args:
    text (str): The text content of the resume.

    Returns:
    str: A comma-separated string of extracted skills.
    """
    # List of common skills (it can be changed as needed)
    skills_keywords = [
        "Python",
        "Java",
        "C++",
        "JavaScript",
        "SQL",
        "NoSQL",
        "Machine Learning",
        "Data Science",
        "Deep Learning",
        "TensorFlow",
        "Keras",
        "PyTorch",
        "NLP",
        "Docker",
        "Kubernetes",
        "R",
        "Scala",
        "Go",
        "Ruby",
        "Swift",
        "C#",
        "PHP",
        "HTML",
        "CSS",
        "React",
        "Angular",
        "Vue.js",
        "Node.js",
        "AWS",
        "Azure",
        "GCP",
        "Hadoop",
        "Spark",
        "MapReduce",
        "Git",
        "GitHub",
        "GitLab",
        "CI/CD",
        "Jenkins",
        "Ansible",
        "Chef",
        "Puppet",
        "Agile",
        "Scrum",
        "DevOps",
        "RESTful APIs",
        "GraphQL",
        "Blockchain",
        "Artificial Intelligence",
        "Data Visualization",
        "Tableau",
        "Power BI",
        "Excel",
        "SQL Server",
        "MongoDB",
        "PostgreSQL",
        "MySQL",
        "SQLite",
        "Elasticsearch",
        "Redis",
        "Spring Boot",
        "Vue.js",
        "Flutter",
        "Android Development",
        "iOS Development",
        "CICD",
        "Linux",
        "Unix",
        "Windows",
        "Cloud Computing",
        "Virtualization",
        "Networking",
        "Cybersecurity",
        "Penetration Testing",
        "Cloud Architecture",
        "System Administration",
        "Software Engineering",
        "Project Management",
        "Leadership",
        "Communication",
        "Teamwork",
        "Problem-Solving",
        "Critical Thinking",
        "Time Management",
        "Negotiation",
        "Presentation Skills",
        "Customer Service",
        "Stakeholder Management",
        "Data Engineering",
        "Data Cleaning",
        "Data Wrangling",
        "Text Mining",
        "Feature Engineering",
        "Predictive Modeling",
        "Statistics",
        "Data Structures",
        "Algorithms",
        "English",
        "French",
        "Spanish",
        "Mandarin",
        "German",
        "Italian",
        "Portuguese",
        "Japanese",
        "Russian",
    ]

    # Convert the text to lowercase for case-insensitive matching
    text = text.lower()

    # Initialize an empty list to store found skills
    extracted_skills = []

    # Look for matches of skill keywords in the text
    for skill in skills_keywords:
        if skill.lower() in text:  # check if the skill is in the resume text
            extracted_skills.append(skill)

    return ", ".join(extracted_skills)  # Return the skills as a comma-separated string


def extract_text_from_pdf(file_path):
    """
    Extract text content from a PDF file using the pdfplumber library.

    Args:
    file_path (str): The path to the PDF file.

    Returns:
    str: The text content extracted from the PDF file.
    """
    text = ""
    try:
        # Open the PDF file and extract text from each page
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text += page.extract_text()
    except Exception as e:
        # If there is an error reading the PDF, print the error message
        print(f"Error reading PDF with pdfplumber: {e}")
    return text


# Route for the home page, serving the index.html file
@app.route("/")
def home():
    return send_from_directory(
        os.path.join(os.path.dirname(os.path.abspath(__file__))), "index.html"
    )


# Route to handle resume file uploads
@app.route("/upload", methods=["POST"])
def upload_resume():
    # Check if the uploaded file is present in the request
    if "resume" not in request.files:
        return jsonify({"message": "No file part"}), 400  # Return error if no file part

    file = request.files["resume"]  # Get the uploaded file

    # Check if the file was not selected
    if file.filename == "":
        return (
            jsonify({"message": "No selected file"}),
            400,
        )  # Return error if no file selected

    # Check if the file type is allowed
    if file and allowed_file(file.filename):
        try:
            # Save the file to the upload folder
            filename = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
            file.save(filename)
            print(f"File saved: {filename}")  # Debugging line

            # Extract text and skills using pdfplumber
            text = extract_text_from_pdf(filename)
            print(f"Extracted Text: {text}")  # Debugging line

            # Extract skills from the text
            skills = extract_skills_from_text(text)
            print(f"Extracted Skills: {skills}")  # Debugging line

            # Create a dictionary with resume details (filename, file path, and extracted skills)
            resume = {
                "filename": file.filename,
                "file_path": filename,
                "skills": skills,
            }

            # Store the resume data in MongoDB
            store_resume(resume)

            # Send the resume data to Kafka (to be processed by a producer)
            produce_resume(resume)

            # Return a success response with extracted skills
            return (
                jsonify({"message": "File uploaded successfully", "skills": skills}),
                200,
            )

        # If there is an error during processing, print the error and return a failure response
        except Exception as e:
            print(f"Error processing resume: {e}")
            return jsonify({"message": f"Error processing resume: {e}"}), 500
    else:
        # Return error if the uploaded file type is not allowed
        return jsonify({"message": "Invalid file type"}), 400


# Run the Flask application in debug mode
if __name__ == "__main__":
    app.run(debug=True)

import json
from kafka import KafkaProducer
from bson import ObjectId


def resume_serializer(resume):
    """
    Custom serializer to handle ObjectId in the resume document.

    Parameters:
    resume (dict): A dictionary representing the resume.

    Returns:
    dict: The resume with ObjectId values converted to string format.
    """
    # Convert ObjectId to string if it exists in the resume dictionary
    if isinstance(resume, dict):
        for key, value in resume.items():
            if isinstance(value, ObjectId):
                resume[key] = str(value)  # Convert ObjectId to string for serialization
    return resume


def produce_resume(resume):
    """
    Sends a resume to a Kafka topic for further processing.

    Parameters:
    resume (dict): A dictionary representing the resume to be sent to Kafka.

    Returns:
    None
    """
    # Serialize the resume with ObjectId handled
    resume = resume_serializer(resume)

    # Initialize the Kafka producer to send the resume data
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],  # Kafka service running in Docker
        value_serializer=lambda v: json.dumps(v).encode(
            "utf-8"
        ),  # Convert resume to JSON
    )

    # Send the serialized resume to the Kafka "resume_topic" topic
    producer.send("resume_topic", value=resume)
    producer.flush()  # Ensure all messages are sent
    producer.close()  # Close the producer after sending the message

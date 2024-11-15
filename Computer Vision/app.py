import streamlit as st
import cv2
import numpy as np
import requests
import time
import logging
import json
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Plate Recognizer API configuration
PLATE_RECOGNIZER_API_KEY = "YOUR_API_KEY"  # Replace with your API key
PLATE_RECOGNIZER_URL = "https://api.platerecognizer.com/v1/plate-reader/"

def detect_plate(image_bytes):
    """
    Send image to Plate Recognizer API and get results
    """
    try:
        response = requests.post(
            PLATE_RECOGNIZER_URL,
            files=dict(upload=image_bytes),
            headers={'Authorization': f'Token {PLATE_RECOGNIZER_API_KEY}'}
        )
        response.raise_for_status()  # Raise exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise e

def process_detection_results(results):
    """
    Process the results from Plate Recognizer API
    """
    if not results.get('results'):
        return "No license plate detected"
    
    plates = []
    for result in results['results']:
        plate = {
            'plate': result['plate'],
            'confidence': result['score'],
            'region': result.get('region', {}).get('code', 'Unknown'),
            'vehicle_type': result.get('vehicle', {}).get('type', 'Unknown')
        }
        plates.append(plate)
    
    return plates

def capture_and_detect():
    logger.info("Camera started.")
    cap = cv2.VideoCapture(0)
    
    if not cap.isOpened():
        logger.error("Camera failed to initialize!")
        st.error("Camera failed to initialize!")
        return
    
    logger.info("Camera initialization successful.")
    frame_count = 0
    
    # Create placeholders for updating content
    frame_placeholder = st.empty()
    info_placeholder = st.empty()
    table_placeholder = st.empty()
    
    # Create a list to store detection history
    if 'detection_history' not in st.session_state:
        st.session_state.detection_history = []
    
    try:
        while st.session_state.running:
            ret, frame = cap.read()
            if not ret:
                logger.error("Failed to capture frame!")
                st.error("Failed to capture frame!")
                break
            
            # Display the frame
            frame_placeholder.image(frame, channels="BGR", caption="Camera Stream", use_container_width=True)
            
            # Process every 30 frames (adjust as needed)
            if frame_count % 30 == 0:
                logger.info("Processing frame for number plate detection.")
                info_placeholder.write("Analyzing frame for number plates...")
                
                try:
                    # Convert frame to bytes
                    _, buffer = cv2.imencode('.jpg', frame)
                    image_bytes = buffer.tobytes()
                    
                    # Send to Plate Recognizer API
                    results = detect_plate(image_bytes)
                    plates = process_detection_results(results)
                    
                    # Update detection history
                    if isinstance(plates, list) and plates:
                        for plate in plates:
                            detection = {
                                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                'plate_number': plate['plate'],
                                'confidence': f"{plate['confidence']:.2f}",
                                'region': plate['region'],
                                'vehicle_type': plate['vehicle_type']
                            }
                            st.session_state.detection_history.insert(0, detection)
                            
                            # Keep only last 10 detections
                            if len(st.session_state.detection_history) > 10:
                                st.session_state.detection_history.pop()
                    
                    # Display current detection
                    if isinstance(plates, list) and plates:
                        info_placeholder.success("Plate(s) detected!")
                        for plate in plates:
                            info_placeholder.write(f"""
                            - Plate Number: {plate['plate']}
                            - Confidence: {plate['confidence']:.2f}
                            - Region: {plate['region']}
                            - Vehicle Type: {plate['vehicle_type']}
                            """)
                    else:
                        info_placeholder.info("No plates detected in this frame")
                    
                    # Display detection history table
                    if st.session_state.detection_history:
                        table_placeholder.write("Recent Detections:")
                        table_placeholder.table(st.session_state.detection_history)
                
                except Exception as e:
                    error_msg = f"Error in processing the request: {str(e)}"
                    logger.error(error_msg)
                    info_placeholder.error(error_msg)
            
            frame_count += 1
            time.sleep(0.1)  # Add delay to reduce load
    
    finally:
        cap.release()
        logger.info("Camera released.")

def main():
    st.title("Vehicle Number Plate Detection")
    st.write("""
    This application uses your camera to detect and recognize vehicle license plates in real-time.
    The detection history shows the last 10 detected plates.
    """)

    # Initialize session state
    if 'running' not in st.session_state:
        st.session_state.running = False
    
    # Add API key input
    api_key = st.sidebar.text_input("Enter Plate Recognizer API Key", type="password")
    if api_key:
        global PLATE_RECOGNIZER_API_KEY
        PLATE_RECOGNIZER_API_KEY = api_key

    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Start Camera", key="start_camera", disabled=st.session_state.running):
            if not PLATE_RECOGNIZER_API_KEY or PLATE_RECOGNIZER_API_KEY == "YOUR_API_KEY":
                st.error("Please enter your Plate Recognizer API key in the sidebar first!")
                return
            st.session_state.running = True
            st.write("Opening the camera...")
            capture_and_detect()
    
    with col2:
        if st.button("Stop", key="stop_camera", disabled=not st.session_state.running):
            st.session_state.running = False
            st.write("Stopping the camera...")
            st.experimental_rerun()
    
    # Add clear history button
    if st.button("Clear Detection History"):
        st.session_state.detection_history = []
        st.experimental_rerun()

if __name__ == "__main__":
    main()
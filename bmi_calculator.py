import streamlit as st

def calculate_bmi(weight, height):
    BMI = round((weight * 703) / (height * height), 2)
    if BMI > 0:
        if BMI < 18.5:
            return BMI, "Underweight", "Minimal"
        elif BMI <= 24.9:
            return BMI, "Normal Weight", "Minimal"
        elif BMI <= 29.9:
            return BMI, "Overweight", "Increased"
        elif BMI <= 34.9:
            return BMI, "Obese", "High"
        elif BMI <= 39.9:
            return BMI, "Severely Obese", "Very High"
        else:
            return BMI, "Morbidly Obese", "Extremely High"
    else:
        return None, None, None

def main():
    st.set_page_config(page_title="BMI Calculator", page_icon="💪")
    st.title("Body Mass Index (BMI) Calculator")

    # User inputs
    name = st.text_input("Enter your name:")
    weight = st.number_input("Enter your weight in lbs:", min_value=1.0)
    height = st.number_input("Enter your height in inches:", min_value=1.0)

    # Calculate BMI when the user clicks the button
    if st.button("Calculate BMI"):
        if name and weight and height:
            BMI, classification, health_risk = calculate_bmi(weight, height)
            if BMI:
                st.success(f"Hello {name}, your BMI is **{BMI}**.")
                st.info(f"Classification: **{classification}**")
                st.warning(f"Health Risk: **{health_risk}**")
            else:
                st.error("Please enter valid input.")
        else:
            st.error("Please fill in all fields.")

if __name__ == "__main__":
    main()

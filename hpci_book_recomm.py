import streamlit as st
import pandas as pd

st.write("""
## Book recommender
#### Welcome! It's time to Read some Books!!
""")
book_df=pd.read_csv('book_index.csv')

title = st.selectbox(
    f'What is your favorite Book',
    book_df['book_name'])

st.write('You selected:', title)

index = book_df.index[book_df['book_name']==title]
print()
book_list = book_df.iloc[index,1:11].values.tolist()[0]
#new_list = [(x.split('-')[0],x.split('-')[1]) for x in book_list ]
new_list = []
for x in book_list:
    try:
      new_list.append((x.split('-')[0],str(round(float(x.split('-')[1]),2))))
    except:
        continue
print(new_list)
if st.button('Get recommendations'):
    df = pd.DataFrame(
     #data = { 'Book Name - Likability (%)' :book_list}
     new_list,
     columns=('Book Name', 'Likability %')
    )

    hide_table_row_index = """
        <style>
        thead tr th:first-child {display:none}
        tbody th {display:none}
        </style>
        """
    # Inject CSS with Markdown
    st.markdown(hide_table_row_index, unsafe_allow_html=True)

    st.table(df)

st.write("""
---
""")

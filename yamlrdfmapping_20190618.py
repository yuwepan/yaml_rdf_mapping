#!/usr/bin/env python
# coding: utf-8

# In[633]:


import yaml
import rdflib
import pprint
import re
import difflib
import logging
import urllib
import pandas as pd

from collections import Iterable


# In[634]:


# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------

# Globals for yaml import
yaml_path = "acm_buendel.yaml"
list_yaml_keys = []
list_yaml_values = []
acm_buendel_yaml ={}
new_yaml_dict ={}
list_yaml_modelnames = []
list_yaml_models_pro=[]
list_yaml_attr = []
list_yaml_attr_pro =[]

# Globals for rdf import
rdf_path = "pdm-data-model.ttl"

pdm_data_model_graph = rdflib.Graph()

dict_matched_classes={}
dict_matched_attr={}
dict_matched_object={}

# sparql for remote import pdm data model 
_pdm_data_model_query = """
prefix pdmm: <https://pdm.app.corpintra.net/datalayer/ontology/pdm/v1#>
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

construct { ?c a ?type ; rdfs:label ?l ;
      rdfs:domain ?d ;
      rdfs:range ?r . }
where {
  graph ?g {
  ?c a ?type ;
    rdfs:label ?l .
   optional {
    ?c rdfs:domain ?d ;
      rdfs:range ?r .
    }
  }
  filter(?type in (owl:Class, owl:DatatypeProperty,owl:ObjectProperty)) .
  ?g a pdmm:PdmDataModel .
}
"""


# In[635]:


# read yaml file
def import_yaml(filepath):
    with open(filepath, 'r') as stream:
        try:
            buendel_yaml = yaml.safe_load(stream)
            #print(acm_buendel_yaml)
        except yaml.YAMLError as exc:
            print(exc)
    return buendel_yaml

acm_buendel_yaml=import_yaml(yaml_path)


# In[636]:


# build imported yaml file into a dictionary. 
# Each item's key is the name of model in .yaml,values are the attriburtes within the model
def iterdict(d):  
    if isinstance (d, dict):
        for k,v in d.items():
            if type(v) is dict:
                if 'type' in v:
                    if v['type'] == 'object':
                        temp_keys = list(v['properties'].keys())
                        list_yaml_keys.append(temp_keys)
                        new_yaml_dict[k]= temp_keys
                        print(k,v['properties'].keys())
                        #print(temp_keys)
                        

            iterdict(v)
    return 


# In[637]:


iterdict(acm_buendel_yaml)


# In[638]:


# preprocessing modelnames in yaml file to separate strings begins with capital letter in words and 
# delete nonuseful words (e.g. BuendelResponse -> Buendel)
def process_yaml_names (list_names):
    list_pro = []

    for word in list_names:
        
        if not word.islower() and not word.isupper():
            word_separate_list = re.findall('[A-Z][^A-Z]*', word)
            for i in word_separate_list:
                if i not in ('And','Response','Array'): 
                    list_pro.append(i)
                #list_yaml_models_pro = [e for e in list_yaml_models_pro if e in ('And', 'Response','Array')]
        else:
            list_pro.append(word)
        
    if 'de' in list_pro: list_pro.remove('de')
            
    return list(set(list_pro))

list_yaml_modelnames = list(new_yaml_dict.keys())
list_yaml_models_pro = process_yaml_names(list_yaml_modelnames)

list_yaml_attr = list(new_yaml_dict.values())
list_yaml_attr_pro= [val for sublist in list_yaml_attr for val in sublist]

while 'de' in list_yaml_attr_pro: list_yaml_attr_pro.remove('de')
while 'en' in list_yaml_attr_pro: list_yaml_attr_pro.remove('en')  


# In[639]:


# remote load pdm data model

def import_rdf_file():
    graph = rdflib.Graph()

    fuseki_sparql_endpoint = "https://triple-store.test.caas.rd.corpintra.net/pdm-data-model/sparql"

    url = fuseki_sparql_endpoint + "?query=" + urllib.parse.quote(_pdm_data_model_query)
    
    graph.parse(url)
    return graph

pdm_data_model_graph= import_rdf_file()


# In[640]:


for stmt in pdm_data_model_graph:
     pprint.pprint(stmt)


# In[641]:


# extract class/datatypeproperty/objectproperty names from rdf file
# build dict of pdm_data_model_graph, with keys = german descrption of class/datatypeproperty/objectproperty 
# values = extracted ClassName, DataTypePropertyName or ObjectPerpertyName
# 3 dictionaries are built here, for: class, datatypeproperty, objectproperty
dict_pdm_class ={}
dict_pdm_object ={}
dict_pdm_data ={}

def create_dict_pdmdatamodel(graph):
    dict_pdm_data_model_class = {}
    dict_pdm_data_model_object = {}
    dict_pdm_data_model_data = {}
    
    #list_of_subjects = pdm_data_model_graph.subjects(predicate=None, object=None)
    list_of_subjects_class = pdm_data_model_graph.subjects(predicate=None,object=rdflib.term.URIRef('http://www.w3.org/2002/07/owl#Class'))
    list_of_subjects_class = set(list_of_subjects_class)
    print(len(list_of_subjects_class))
    
    list_of_subjects_objectproperty = pdm_data_model_graph.subjects(predicate=None,object=rdflib.term.URIRef('http://www.w3.org/2002/07/owl#ObjectProperty'))
    list_of_subjects_objectproperty = set(list_of_subjects_objectproperty)
    
    list_of_subjects_datatypeproperty = pdm_data_model_graph.subjects(predicate=None,object=rdflib.term.URIRef('http://www.w3.org/2002/07/owl#DatatypeProperty'))
    list_of_subjects_datatypeproperty = set(list_of_subjects_datatypeproperty)

    
    for pdm_data_model_attribute in list_of_subjects_class:
        # get the de-label of each subject, returned as pair
        couple_de_label = pdm_data_model_graph.preferredLabel(pdm_data_model_attribute,lang='de')
        couple_en_label = pdm_data_model_graph.preferredLabel(pdm_data_model_attribute,lang='en')
        
        if len(couple_de_label)!=0:
            str_de_label = str(couple_de_label[0][1])
            
            # return de-label without space, convinent for matching, used later as 'keys' in dict_pdm_data_model
            str_de_label = str_de_label.replace(" ", "")
        
            str_attr_temp = str(pdm_data_model_attribute)
       
            # return attribute name, used later as 'values'
            if '/' in str_attr_temp: 
                #str_attribute = str_attr_temp.split('#')[1]
                str_attribute = str_attr_temp.split("/")[-1]
                
                temp_dict_item = dict()
                temp_dict_item[str_de_label]=str_attribute
                
                for k,v in temp_dict_item.items():
                    if k in dict_pdm_data_model_class:
                        dict_pdm_data_model_class[k].append(v)
                    else:
                        dict_pdm_data_model_class[k] = [v]   
    
    for pdm_data_model_attribute in list_of_subjects_objectproperty:
        # get the de-label of each subject, returned as pair
        couple_de_label = pdm_data_model_graph.preferredLabel(pdm_data_model_attribute,lang='de')

        
        if len(couple_de_label)!=0:
            str_de_label = str(couple_de_label[0][1])
            # return de-label without space, convinent for matching, used later as 'keys' in dict_pdm_data_model
            str_de_label = str_de_label.replace(" ", "")
        
            str_attr_temp = str(pdm_data_model_attribute)
         
            # return attribute name, used later as 'values'
            if '/' in str_attr_temp: 
                #str_attribute = str_attr_temp.split('#')[1]
                str_attribute = str_attr_temp.split("/")[-1]
                
                temp_dict_item = dict()
                temp_dict_item[str_de_label]=str_attribute
                
                for k,v in temp_dict_item.items():
                    if k in dict_pdm_data_model_object:
                        dict_pdm_data_model_object[k].append(v)
                    else:
                        dict_pdm_data_model_object[k] = [v]
    

    for pdm_data_model_attribute in list_of_subjects_datatypeproperty:
        # get the de-label of each subject, returned as pair
        couple_de_label = pdm_data_model_graph.preferredLabel(pdm_data_model_attribute,lang='de')
        #print(couple_de_label)
        
        if len(couple_de_label)!=0:
            str_de_label = str(couple_de_label[0][1])
            # return de-label without space, convinent for matching, used later as 'keys' in dict_pdm_data_model
            str_de_label = str_de_label.replace(" ", "")
        
            str_attr_temp = str(pdm_data_model_attribute)
            #print(str_attr_temp)
            # return attribute name, used later as 'values'
            if '/' in str_attr_temp: 
                #str_attribute = str_attr_temp.split('#')[1]
                str_attribute = str_attr_temp.split("/")[-1]
                
                temp_dict_item = dict()
                temp_dict_item[str_de_label]=str_attribute
                #print(temp_dict_item)
                
                for k,v in temp_dict_item.items():
                    if k in dict_pdm_data_model_data:
                        dict_pdm_data_model_data[k].append(v)
                    else:
                        dict_pdm_data_model_data[k] = [v]
    
    return dict_pdm_data_model_class,dict_pdm_data_model_object, dict_pdm_data_model_data


# In[642]:


[dict_pdm_class,dict_pdm_object,dict_pdm_data] = create_dict_pdmdatamodel(pdm_data_model_graph)


# In[ ]:





# In[ ]:





# In[643]:


def matching_yaml_class():
    matched_pdm_classes=[]
    list_of_matched_class=[]
    dict_tmp_matched_class={}

    for i in list_yaml_models_pro:
    
        list_of_matched_class = difflib.get_close_matches(i,list(dict_pdm_class.keys()),cutoff=0.65)
        if list_of_matched_class !=[]:
            
            for j in list_of_matched_class:
                #matched_pdm_classes is a list of founded de_lables of attributes in pdm data model
                matched_pdm_classes.append(j)
                tmp = dict_pdm_class[j]
                dict_tmp_matched_class.setdefault(i, []).append(tmp)
    return dict_tmp_matched_class


# In[644]:


dict_matched_class = matching_yaml_class()


# In[645]:


# flatten list, convert nested list to list
def flatten(lis):
     for item in lis:
        if isinstance(item, Iterable) and not isinstance(item, str):
            for x in flatten(item):
                yield x
        else:        
             yield item

# get the class of datatypeproperty
def get_domain(name_datatypeprop):
    attr_predictate = rdflib.term.URIRef('http://www.w3.org/2000/01/rdf-schema#domain')
    attr_subject = rdflib.term.URIRef('https://pdm.app.corpintra.net/datalayer/model/pdm/v1/'+ name_datatypeprop)
    domain_tmp = pdm_data_model_graph.objects(subject=attr_subject,predicate=attr_predictate)

    list_domain = list(set(domain_tmp))
    return list_domain

def get_range(name_objectprop):
    attr_predictate = rdflib.term.URIRef('http://www.w3.org/2000/01/rdf-schema#range')
    attr_subject = rdflib.term.URIRef('https://pdm.app.corpintra.net/datalayer/model/pdm/v1/'+ name_objectprop)
    range_tmp = pdm_data_model_graph.objects(subject=attr_subject,predicate=attr_predictate)

    list_range = list(set(range_tmp))
    return list_range


# In[646]:


def matching_yaml_rdf_attribute():
    matched_pdm_datatypes=[]
    list_of_matched_datatype=[]
    dict_tmp_matched_attr={}

    for i in list_yaml_attr_pro:
        #list_of_matched_datatypes is a list of matched de-labels in pdm data model
        list_of_matched_datatype = difflib.get_close_matches(i,list(dict_pdm_data.keys()),cutoff=0.65)
        if list_of_matched_datatype !=[]:
            
            for j in list_of_matched_datatype:
                #matched_pdm_datatypes is a list of founded de_lables of attributes in pdm data model
                matched_pdm_datatypes.append(j)
                tmp = dict_pdm_data[j]
                dict_tmp_matched_attr.setdefault(i, []).append(tmp)
    
    # flatten the values in dictionary, so each key (attribute name in yaml file) 
    # ..corresonds to a list of attributes in pdm data model
    for k, v in dict_tmp_matched_attr.items():
        tmp = list(set(list(flatten(dict_tmp_matched_attr[k]))))
        dict_tmp_matched_attr[k] = tmp
    return dict_tmp_matched_attr


# In[647]:


dict_matched_attr = matching_yaml_rdf_attribute()


#pd.DataFrame.from_dict(dict_matched_attr)


# In[648]:


def matching_yaml_rdf_object():
    matched_pdm_object=[]
    list_of_matched_object=[]
    dict_tmp_matched_object={}

    for i in list_yaml_attr_pro:
        #list_of_matched_datatypes is a list of matched de-labels in pdm data model
        list_of_matched_object = difflib.get_close_matches(i,list(dict_pdm_object.keys()),cutoff=0.65)
        if list_of_matched_object !=[]:
            
            for j in list_of_matched_object:
                #matched_pdm_datatypes is a list of founded de_lables of attributes in pdm data model
                matched_pdm_object.append(j)
                tmp = dict_pdm_object[j]
                dict_tmp_matched_object.setdefault(i, []).append(tmp)
    
    # flatten the values in dictionary, so each key (attribute name in yaml file) 
    # ..corresonds to a list of attributes in pdm data model
    for k, v in dict_tmp_matched_object.items():
        tmp = list(set(list(flatten(dict_tmp_matched_object[k]))))
        dict_tmp_matched_object[k] = tmp

    return dict_tmp_matched_object


# In[649]:


dict_matched_object = matching_yaml_rdf_object()


# In[650]:


#get key of dict based on value
#{k:v for k, v in new_yaml_dict.items() if 'buendelNummer' in v}


# In[651]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





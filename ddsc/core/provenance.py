from __future__ import absolute_import, print_function
import json
from ddsc.core.util import KindType


def upload_provenance(data_service, filename):
    with open(filename, 'r') as infile:
        document = ProvDocument(json.load(infile))
        lookup = {}
        for activity in document.get_activities():
            activity.save(lookup, data_service)
            lookup[activity.name] = activity
        for entity in document.get_entities():
            lookup[entity.name] = entity
        for item in document.get_used():
            item.save(lookup, data_service)
        for item in document.get_was_derived_from():
            item.save(lookup, data_service)
        for item in document.get_was_generated_by():
            item.save(lookup, data_service)


class ProvAttr(object):
    START_TIME = 'prov:startTime'
    END_TIME = 'prov:endTime'
    LABEL = 'prov:label'
    TYPE = 'prov:type'
    ACTIVITY = 'prov:activity'
    ENTITY = 'prov:entity'
    GENERATED_ENTITY = 'prov:generatedEntity'
    USED_ENTITY = 'prov:usedEntity'


class DDSAttr(object):
    DDS_FILE_VERSION = 'ddsfile:ver'


class ProvDocument(object):
    def __init__(self, doc):
        self.doc = doc

    def get_activities(self):
        return self._doc_dict_to_list("activity", ProvActivity)

    def get_entities(self):
        return self._doc_dict_to_list("entity", ProvEntity)

    def get_used(self):
        return self._doc_dict_to_list("used", ProvUsed)

    def get_was_derived_from(self):
        return self._doc_dict_to_list("wasDerivedFrom", ProvWasDerivedFrom)

    def get_was_generated_by(self):
        return self._doc_dict_to_list("wasGeneratedBy", ProvWasGeneratedBy)

    def _doc_dict_to_list(self, doc_field_name, constructor_func):
        doc_dict = self.doc.get(doc_field_name)
        items = []
        for key in doc_dict.keys():
            items.append(constructor_func(key, doc_dict[key]))
        return items


class ProvActivity(object):
    def __init__(self, name, props):
        self.id = None
        self.name = name
        self.start_time = props.get(ProvAttr.START_TIME)
        self.end_time = props.get(ProvAttr.END_TIME)
        self.props = props

    def save(self, lookup, data_service):
        response = data_service.create_activity(self.name, started_on=self.start_time, ended_on=self.end_time)
        self.id = response.json()['id']


class ProvEntity(object):
    def __init__(self, name, props):
        self.name = name
        self.label = props.get(ProvAttr.LABEL)
        self.type = props.get(ProvAttr.TYPE)
        self.props = props


class ProvUsed(object):
    def __init__(self, name, props):
        self.id = None
        self.name = name
        self.activity = props[ProvAttr.ACTIVITY]
        self.entity = props[ProvAttr.ENTITY]
        self.props = props

    def save(self, lookup, data_service):
        activity = lookup.get(self.activity)
        file_entity = lookup.get(self.entity)
        response = data_service.create_used_relation(activity.id, KindType.file_str,
                                                     file_entity.props.get(DDSAttr.DDS_FILE_VERSION))
        self.id = response.json()['id']


class ProvWasDerivedFrom(object):
    def __init__(self, name, props):
        self.id = None
        self.name = name
        self.generated_entity = props[ProvAttr.GENERATED_ENTITY]
        self.used_entity = props[ProvAttr.USED_ENTITY]
        self.props = props

    def save(self, lookup, data_service):
        used_entity = lookup.get(self.used_entity)
        generated_entity = lookup.get(self.generated_entity)
        response = data_service.create_was_derived_from_relation(
            used_entity.props.get(DDSAttr.DDS_FILE_VERSION), KindType.file_str,
            generated_entity.props.get(DDSAttr.DDS_FILE_VERSION), KindType.file_str)
        self.id = response.json()['id']


class ProvWasGeneratedBy(object):
    def __init__(self, name, props):
        self.id = None
        self.name = name
        self.activity = props[ProvAttr.ACTIVITY]
        self.entity = props[ProvAttr.ENTITY]
        self.props = props

    def save(self, lookup, data_service):
        activity = lookup.get(self.activity)
        file_entity = lookup.get(self.entity)
        response = data_service.create_was_generated_by_relation(activity.id, KindType.file_str,
                                                                 file_entity.props.get(DDSAttr.DDS_FILE_VERSION))
        self.id = response.json()['id']

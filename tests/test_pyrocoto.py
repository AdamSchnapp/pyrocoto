import pytest
from xml.etree.ElementTree import Element, tostring
from pyrocoto import cyclestr

def test_cyclestr():
    with pytest.raises(ValueError, match="passed element does not have text"):
        E = Element('task')
        cyclestr(E)
    with pytest.raises(ValueError, match="offset was passed but no '@' in element.text"):
        E = Element('task')
        E.text = 'stuff'
        cyclestr(E, offset='01:00')
    with pytest.raises(ValueError, match="offset passed must be of type str"):
        E = Element('task')
        E.text = 'stuff_at_@Y'
        cyclestr(E, offset=1)
    E = Element('task')
    E.text = 'stuff_at_@Y'
    print(E)
    assert tostring(cyclestr(E, offset='01:00')) == b'<task><cyclestr offset="01:00">stuff_at_@Y</cyclestr></task>'

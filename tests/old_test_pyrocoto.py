import pytest
from xml.etree.ElementTree import Element, tostring
from pyrocoto import cyclestr, CycleDefinition 


@pytest.fixture
def E():
    return Element('task')


def test_cyclestr_without_element():
    with pytest.raises(ValueError, match="must be of type Element"):
        cyclestr('not_element')


def test_cyclestr_without_text(E):
    with pytest.raises(ValueError, match="passed element does not have text"):
        cyclestr(E)


def test_cyclestr_with_offset_and_no_cyclestr_info(E):
    with pytest.raises(ValueError, match="offset was passed but no '@' in element.text"):
        E.text = 'stuff'
        cyclestr(E, offset='01:00')


def test_cyclestr_with_non_string_offset(E):
    with pytest.raises(ValueError, match="offset passed must be of type str"):
        E.text = 'stuff_at_@Y'
        cyclestr(E, offset=1)


def test_cyclestr_returns_expected_result(E):
    E.text = 'stuff_at_@Y'
    assert tostring(cyclestr(E, offset='01:00')) == b'<task><cyclestr offset="01:00">stuff_at_@Y</cyclestr></task>'


def test_CycleDefinition_eq():
    C1 = CycleDefinition('hourly', '0 * * * * *', activation_offset='-15:00')
    C2 = CycleDefinition('hourly', '0 * * * * *', activation_offset='-15:00')
    assert C1 == C2
    C2 = CycleDefinition('hourly', '30 * * * * *', activation_offset='-15:00')
    assert C1 != C2

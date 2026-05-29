[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "304b1974-676d-48af-b4e4-8cf6f0fe139e"]}[Data]{[warehouseId = "01061958-0005-4173-9e94-0ac8d54716e1"]}[Data];
[BindToDefaultDestination = true]
shared Silver_Locations = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "304b1974-676d-48af-b4e4-8cf6f0fe139e"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "01061958-0005-4173-9e94-0ac8d54716e1"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "Bronze_Locations"]}[Data],
  #"Split column by delimiter" = Table.SplitColumn(#"Navigation 2", "manager_name", Splitter.SplitTextByDelimiter(" "), {"manager_name.1", "manager_name.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Split column by delimiter", {{"manager_name.1", "manager_first_name"}, {"manager_name.2", "manager_surname"}})
in
  #"Renamed columns";

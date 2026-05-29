[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "304b1974-676d-48af-b4e4-8cf6f0fe139e"]}[Data]{[warehouseId = "01061958-0005-4173-9e94-0ac8d54716e1"]}[Data];
[BindToDefaultDestination = true]
shared Silver_Addresses = let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/LearnMicrosoftFabric/dojo/main/datasets/contacts.csv"), [Delimiter = ",", Columns = 8, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"id", Int64.Type}, {"location_id", Int64.Type}, {"organization_id", Int64.Type}, {"service_id", Int64.Type}, {"name", type text}, {"title", type text}, {"email", type text}, {"department", type text}}),
  #"Split column by delimiter" = Table.SplitColumn(#"Changed column type", "name", Splitter.SplitTextByDelimiter(" "), {"name.1", "name.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Split column by delimiter", {{"name.1", "first_name"}, {"name.2", "surname"}})
in
  #"Renamed columns";

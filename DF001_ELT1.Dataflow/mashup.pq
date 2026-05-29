[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "304b1974-676d-48af-b4e4-8cf6f0fe139e"]}[Data]{[warehouseId = "01061958-0005-4173-9e94-0ac8d54716e1"]}[Data];
[BindToDefaultDestination = true]
shared Bronze_Locations = let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/LearnMicrosoftFabric/dojo/main/datasets/locations2.csv"), [Delimiter = ",", Columns = 9, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"id", Int64.Type}, {"organization_id", Int64.Type}, {"accessibility", type text}, {"admin_emails", type text}, {"alternate_name", type text}, {"description", type text}, {"email", type text}, {"location_name", type text}, {"manager_name", type text}})
in
  #"Changed column type";

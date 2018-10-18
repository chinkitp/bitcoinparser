using System.Collections.Generic;
using System.Collections.ObjectModel;


public abstract class Epm
{
    private readonly Dictionary<string, object> _props;
    public string Id { get; }
    public  Meta Meta { get; }
    public IReadOnlyDictionary<string, object> Data => _props;

    public Epm(string id, string label, Dictionary<string, object> props)
    {
        Id = id;
        Meta = new Meta(label);
        _props = props;
    }
}

public class Meta
{
    public Meta(string label)
    {
        Label = label;
    }

    public string Label { get; }
    public readonly string[] Graphs = new []{"Bitcion"};
}

public class Vertex : Epm
{
    public Vertex(string id, string label, Dictionary<string, object> props) : base(id, label, props)
    {
    }
}

public class Edge : Epm
{
    public Edge(string id, string label, string source, string target, Dictionary<string,object> props) : base(id, label, props)
    {
        Source = source;
        Target = target;
    }
    public string Source { get; }
    public string Target { get; }
}
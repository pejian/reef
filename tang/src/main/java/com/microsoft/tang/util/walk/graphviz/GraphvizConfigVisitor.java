/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.tang.util.walk.graphviz;

import com.microsoft.tang.Configuration;

import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.types.NamedParameterNode;

import com.microsoft.tang.util.walk.Walk;
import com.microsoft.tang.util.walk.EdgeVisitor;
import com.microsoft.tang.util.walk.AbstractConfigNodeVisitor;

/**
 * Build a Graphviz representation of the configuration graph.
 */
public final class GraphvizConfigVisitor
    extends AbstractConfigNodeVisitor implements EdgeVisitor<Node>
{

  /** Legend for the configuration graph in Graphviz format */
  private static final String LEGEND =
    "  subgraph cluster_legend {\n"
    + "    label=\"Legend\";\n"
    + "    shape=box;\n"
    + "    subgraph cluster_1 {\n"
    + "      style=invis; label=\"\";\n"
    + "      ex1l [shape=point, label=\"\"]; ex1r [shape=point, label=\"\"];\n"
    + "      ex2l [shape=point, label=\"\"]; ex2r [shape=point, label=\"\"];\n"
    + "      ex3l [shape=point, label=\"\"]; ex3r [shape=point, label=\"\"];\n"
    + "      ex4l [shape=point, label=\"\"]; ex4r [shape=point, label=\"\"];\n"
    + "      ex1l -> ex1r [style=solid, dir=back, arrowtail=diamond, label=\"contains\"];\n"
    + "      ex2l -> ex2r [style=dashed, dir=back, arrowtail=empty, label=\"implements\"];\n"
    + "      ex3l -> ex3r [style=\"dashed,bold\", dir=back, arrowtail=empty, label=\"external\"];\n"
    + "      ex4l -> ex4r [style=solid, dir=back, arrowtail=normal, label=\"binds\"];\n"
    + "    }\n"
    + "    subgraph cluster_2 {\n"
    + "      style=invis; label=\"\";\n"
    + "      PackageNode [shape=folder];\n"
    + "      ClassNode [shape=box];\n"
    + "      Singleton [shape=box, style=filled];\n"
    + "      NamedParameterNode [shape=oval];\n"
    + "    }\n"
    + "  }\n";

  /** Accumulate string representation of the graph here. */
  private final transient StringBuilder graphStr = new StringBuilder(
    "digraph ConfigMain {\n"
    + "  rankdir=LR;\n");

  /**
   * Entire TANG configuration object.
   */
  private final transient Configuration config;

  /**
   * If true, plot IS-A edges for know implementations.
   */
  private final transient boolean showImpl;

  /**
   * Create a new TANG configuration visitor.
   * @param aConfig Entire TANG configuration object.
   * @param aShowImpl If true, plot IS-A edges for know implementations.
   * @param aShowLegend If true, add legend to the plot.
   */
  public GraphvizConfigVisitor(final Configuration config,
          final boolean showImpl, final boolean showLegend) {
    super();
    this.config = config;
    this.showImpl = showImpl;
    if (showLegend) {
      this.graphStr.append(LEGEND);
    }
  }

  /**
   * @return TANG configuration represented as a Graphviz DOT string.
   */
  @Override
  public String toString() {
    return this.graphStr.toString() + "}\n";
  }

  /**
   * Process current class configuration node.
   * @param node Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final ClassNode node) {

    this.graphStr
            .append("  ")
            .append(node.getName())
            .append(" [label=\"")
            .append(node.getName())
            .append("\", shape=box")
            .append(config.isSingleton(node) ? ", style=filled" : "")
            .append("];\n");

    final ClassNode boundImplNode = config.getBoundImplementation(node);
    if (boundImplNode != null) {
      this.graphStr
              .append("  ")
              .append(node.getName())
              .append(" -> ")
              .append(boundImplNode.getName())
              .append(" [style=solid, dir=back, arrowtail=normal];\n");
    }

    for (final Object implNodeObj : node.getKnownImplementations()) {
      final ClassNode implNode = (ClassNode) implNodeObj;
      if (implNode != boundImplNode && implNode != node
              && (implNode.isExternalConstructor() || this.showImpl))
      {
        this.graphStr
                .append("  ")
                .append(node.getName())
                .append(" -> ")
                .append(implNode.getName())
                .append(" [style=\"dashed")
                .append(implNode.isExternalConstructor() ? ",bold" : "")
                .append("\", dir=back, arrowtail=empty];\n");
      }
    }

    return true;
  }

  /**
   * Process current package configuration node.
   * @param node Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final PackageNode node) {
    if (!node.getName().isEmpty()) {
      this.graphStr
              .append("  ")
              .append(node.getName())
              .append(" [label=\"")
              .append(node.getFullName())
              .append("\", shape=folder];\n");
    }
    return true;
  }

  /**
   * Process current configuration node for the named parameter.
   * @param node Current configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final NamedParameterNode node) {
    this.graphStr
            .append("  ")
            .append(node.getName())
            .append(" [label=\"")
            .append(node.getSimpleArgName())           // parameter type, e.g. "Integer"
            .append("\\n")
            .append(node.getName())                    // short name, e.g. "NumberOfThreads"
            .append(" = ")
            .append(config.getNamedParameter(node))   // bound value, e.g. "16"
            .append("\\n(default = ")
            .append(node.getDefaultInstanceAsString()) // default value, e.g. "4"
            .append(")\", shape=oval];\n");
    return true;
  }

  /**
   * Process current edge of the configuration graph.
   * @param nodeFrom Current configuration node.
   * @param nodeTo Destination configuration node.
   * @return true to proceed with the next node, false to cancel.
   */
  @Override
  public boolean visit(final Node nodeFrom, final Node nodeTo) {
    if (!nodeFrom.getName().isEmpty()) {
      this.graphStr
              .append("  ")
              .append(nodeFrom.getName())
              .append(" -> ")
              .append(nodeTo.getName())
              .append(" [style=solid, dir=back, arrowtail=diamond];\n");
    }
    return true;
  }

  /**
   * Produce a Graphviz DOT string for a given TANG configuration.
   * @param config TANG configuration object.
   * @param showImpl If true, plot IS-A edges for know implementations.
   * @param showLegend If true, add legend to the plot.
   * @return configuration graph represented as a string in Graphviz DOT format.
   */
  public static String getGraphvizString(final Configuration config,
          final boolean showImpl, final boolean showLegend)
  {
    final GraphvizConfigVisitor visitor = new GraphvizConfigVisitor(config, showImpl, showLegend);
    final Node root = config.getClassHierarchy().getNamespace();
    Walk.preorder(visitor, visitor, root);
    return visitor.toString();
  }
}

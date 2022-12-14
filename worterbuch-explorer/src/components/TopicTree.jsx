import * as React from "react";
import TreeView from "@mui/lab/TreeView";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import TreeItem from "@mui/lab/TreeItem";
import { Typography } from "@mui/material";

export default function TopicTree({ data, separator }) {
  const treeItems = toTreeItems(data, separator);

  return (
    <TreeView
      aria-label="file system navigator"
      defaultCollapseIcon={<ExpandMoreIcon />}
      defaultExpandIcon={<ChevronRightIcon />}
      sx={{
        height: "100vh",
        flexGrow: 1,
        maxWidth: "100vw",
        overflowY: "auto",
      }}
    >
      {treeItems}
    </TreeView>
  );
}

function toTreeItems(data, separator, path) {
  let items = [];

  data.forEach((child, id) => {
    const item = toTreeItem(
      path ? `${path}${separator}${id}` : id,
      child,
      id,
      separator
    );
    items.push(item);
  });

  return <>{items}</>;
}

function toTreeItem(path, item, id, separator) {
  if (!item.value && item.children && item.children.size === 1) {
    const [childId, child] = item.children.entries().next().value;
    return toTreeItem(
      `${path}${separator}${childId}`,
      child,
      `${id}${separator}${childId}`,
      separator
    );
  } else {
    const label = item.value ? (
      <>
        <Typography display="inline-block">{id} = </Typography>
        <Typography
          display="inline-block"
          style={{ fontWeight: 600, marginInlineStart: 4 }}
        >
          {item.value}
        </Typography>
      </>
    ) : (
      <Typography display="inline-block">{id}</Typography>
    );
    return (
      <TreeItem key={path} nodeId={path} label={label}>
        {item.children ? toTreeItems(item.children, separator, path) : null}
      </TreeItem>
    );
  }
}

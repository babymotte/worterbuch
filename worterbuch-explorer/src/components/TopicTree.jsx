import * as React from "react";
import TreeView from "@mui/lab/TreeView";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import TreeItem from "@mui/lab/TreeItem";
import { Typography } from "@mui/material";

export default function TopicTree({ data }) {
  const treeItems = toTreeItems(data);

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

function toTreeItems(data, path) {
  let items = [];

  data.forEach((child, id) => {
    const item = toTreeItem(path ? `${path}/${id}` : id, child, id);
    items.push(item);
  });

  return <>{items}</>;
}

function toTreeItem(path, item, id) {
  if (!item.value && item.children && item.children.size === 1) {
    const [childId, child] = item.children.entries().next().value;
    return toTreeItem(`${path}/${childId}`, child, `${id}/${childId}`);
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
        {item.children ? toTreeItems(item.children, path) : null}
      </TreeItem>
    );
  }
}

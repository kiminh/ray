package org.ray.runtime.util.generator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;

public class RayJobsGenerator extends BaseGenerator {

  private String build() {
    sb = new StringBuilder();

    newLine("// generated automatically, do not modify.");
    newLine("");
    newLine("package org.ray.api.job;");
    newLine("");
    newLine("import org.ray.api.Ray;");
    newLine("import org.ray.api.RayObject;");
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      newLine("import org.ray.api.function.RayFunc" + i + ";");
    }
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      newLine("import org.ray.api.function.RayFuncVoid" + i + ";");
    }
    newLine("import org.ray.api.options.JobOptions;");
    newLine("");

    newLine("/**");
    newLine(" * Util class for submitting jobs, with type-safe interfaces. ");
    newLine(" **/");
    newLine("@SuppressWarnings({\"rawtypes\", \"unchecked\"})");
    newLine("public interface RayJobs {");
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      buildMethod(i, true);
      buildMethod(i, false);
    }
    newLine("}");
    return sb.toString();
  }

  private void buildMethod(int numParameters, boolean hasReturn) {
    // 1) Construct the `genericTypes` part, e.g. `<T0, T1, T2, R>`.
    String genericTypes = "";
    for (int i = 0; i < numParameters; i++) {
      genericTypes += "T" + i + ", ";
    }
    if (hasReturn) {
      genericTypes += "R, ";
    }

    if (!genericTypes.isEmpty()) {
      // Trim trailing ", ";
      genericTypes = genericTypes.substring(0, genericTypes.length() - 2);
      genericTypes = "<" + genericTypes + ">";
    }

    // 2) Construct the `returnType` part.
    String returnType = hasReturn ? "RayJob<R>" : "RayJob<Void>";

    // 3) Construct the `argsDeclaration` part.
    String argsDeclarationPrefix = String.format("RayFunc%s%d%s f, ",
        hasReturn ? "" : "Void",
        numParameters,
        genericTypes);

    // Enumerate all combinations of the parameters.
    for (String param : generateParameters(numParameters)) {
      String argsDeclaration = argsDeclarationPrefix + param;
      argsDeclaration += "JobOptions options, ";
      // Trim trailing ", ";
      argsDeclaration = argsDeclaration.substring(0, argsDeclaration.length() - 2);
      // Print the first line (method signature).
      newLine(1, String.format(
          "static%s %s submitJob(%s) {",
          genericTypes.isEmpty() ? "" : " " + genericTypes, returnType, argsDeclaration
      ));

      // 4) Construct the `args` part.
      String args = "";
      for (int i = 0; i < numParameters; i++) {
        args += "t" + i + ", ";
      }
      // Trim trailing ", ";
      if (!args.isEmpty()) {
        args = args.substring(0, args.length() - 2);
      }
      // Print the second line (local args declaration).
      newLine(2, String.format("Object[] args = new Object[]{%s};", args));

      // 5) Construct the third line.
      newLine(2, "return Ray.internal().submitJob(f, args, options);");
      newLine(1, "}");
    }
  }

  private List<String> generateParameters(int numParams) {
    List<String> res = new ArrayList<>();
    dfs(0, numParams, "", res);
    return res;
  }

  private void dfs(int pos, int numParams, String cur, List<String> res) {
    if (pos >= numParams) {
      res.add(cur);
      return;
    }
    String nextParameter = String.format("T%d t%d, ", pos, pos);
    dfs(pos + 1, numParams, cur + nextParameter, res);
    nextParameter = String.format("RayObject<T%d> t%d, ", pos, pos);
    dfs(pos + 1, numParams, cur + nextParameter, res);
  }

  public static void main(String[] args) throws IOException {
    String path = System.getProperty("user.dir")
        + "/api/src/main/java/org/ray/api/job/RayJobs.java";
    FileUtils.write(new File(path), new RayJobsGenerator().build(),
        Charset.defaultCharset());
  }
}

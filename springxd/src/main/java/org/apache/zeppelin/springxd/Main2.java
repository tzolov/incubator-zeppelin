package org.apache.zeppelin.springxd;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.springframework.xd.rest.client.impl.SpringXDTemplate;
import org.springframework.xd.rest.domain.CompletionKind;

/**
 * @author tzoloc
 *
 */
public class Main2 {

  public static void main(String[] args) throws URISyntaxException {
    SpringXDTemplate xdTemplate = new SpringXDTemplate(new URI("http://ambari.localdomain:9393"));
    String buf = "http | transform --";
    List<String> completions =
        xdTemplate.completionOperations().completions(CompletionKind.stream, buf, 1);

    System.out.println(completions.size());
    for (String c : completions) {
      System.out.println("  " + c);
    }
  }
}
